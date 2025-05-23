from modules.marcxml_parser import MarcXmlParser
import json
import time
import html
import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime

class KohaAPIHarvester:
    def __init__(self, db, parser_config, logger, pause_duration=20, batch_size=10):
        self.db = db
        self.parser = MarcXmlParser(parser_config)
        self.logger = logger
        self.pause_duration = pause_duration
        self.batch_size = batch_size

    def parse_options(self, server):
        try:
            return json.loads(server['Options'])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid JSON in Options for server {server['name']}: {e}")
            raise ValueError("Invalid Options format")

    def authenticate(self, server, options):
        if 'ClientID' in options and 'Secret' in options:
            token_url = f"{server['uri']}oauth/token"
            response = requests.post(token_url, {
                'grant_type': 'client_credentials',
                'client_id': options['ClientID'],
                'client_secret': options['Secret']
            })

            if response.status_code == 200:
                token = response.json().get('access_token')
                self.logger.info(f"Authentication successful for server {server['name']}")
                return token
            else:
                self.logger.error(f"Authentication failed for server {server['name']}: {response.text}")
                raise Exception("Authentication failed")
        else:
            self.logger.info(f"No ClientID and Secret provided for {server['name']}. Using ILS-DI fallback.")
            return None

    def harvest(self, server, record_type):
        options = self.parse_options(server)
        token = self.authenticate(server, options)

        if token:
            self.harvest_koha_api(server, token, record_type)
        else:
            self.harvest_ilsdi(server, record_type)

    def harvest_koha_api(self, server, token, record_type):
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/marcxml+xml'
        }

        url = f"{server['uri']}authorities" if record_type == 'auth' else f"{server['uri']}biblios"

        while True:
            params = {'q': '*'}  # Minimal query to test
            
            #self.logger.debug(f"Querying {url} with params: {params}")
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 403:
                self.logger.error(f"Access denied for server {server['name']}.")
                break
            elif response.status_code == 400:
                self.logger.error(f"Malformed query: {response.json()}")
                break
            elif response.status_code != 200:
                self.logger.error(f"Unexpected HTTP status {response.status_code}: {response.text}")
                break

            records = response.json()
            if not records:
                self.logger.info(f"No more records to harvest for {server['name']}.")
                break

            self.process_records(records, record_type, server)
            # Break if no pagination or cursor mechanism exists
            break
            time.sleep(self.pause_duration)

    def harvest_ilsdi(self, server, record_type):
        ilsdi_url = server['uri'].replace('/api/v1/', '/cgi-bin/koha/ilsdi.pl?service=GetAuthorityRecords&id={}') if record_type == 'auth' \
            else server['uri'].replace('/api/v1/', '/cgi-bin/koha/ilsdi.pl?service=GetBiblioRecords&id={}')

        record_id = 1
        batch_size = self.batch_size
        consecutive_not_found = 0
        max_not_found_threshold = 40
        processed_records = 0
        start_time = time.time()

        while True:
            # Prepare batch of record IDs
            record_ids = "+".join(str(record_id + i) for i in range(batch_size))
            url = ilsdi_url.format(record_ids)

            response = requests.get(url)
            batch_start_time = time.time()

            if response.status_code == 200:
                try:
                    # Register the 'marc' namespace
                    ET.register_namespace("marc", "http://www.loc.gov/MARC21/slim")

                    response_text = clean_text(response.text)
                    # Parse the response as XML
                    root = ET.fromstring(response_text)
                    found_valid = False

                    # Check each <record> element
                    for record_elem in root.findall(".//record"):
                        if record_elem.find("code") is not None and record_elem.find("code").text == "RecordNotFound":
                            continue
                        else:
                            found_valid = True
                            #self.logger.debug(f"Processing valid record: {ET.tostring(record_elem, encoding='unicode')}")
                            self.process_records_ilsdi([record_elem], record_type, server)
                            processed_records += 1

                    if not found_valid:
                        consecutive_not_found += batch_size
                        self.logger.info(f"Consecutive RecordNotFound count: {consecutive_not_found}")
                        if consecutive_not_found >= max_not_found_threshold:
                            self.logger.info("Reached maximum consecutive RecordNotFound threshold. Stopping harvest.")
                            break
                    else:
                        consecutive_not_found = 0  # Reset the counter if valid records are found

                    # Log batch performance
                    batch_duration = time.time() - batch_start_time
                    self.logger.info(f"Batch processed in {batch_duration:.2f} seconds. Total processed records: {processed_records}")

                    # Log performance every 500 records or at the end
                    if processed_records % 500 == 0 or consecutive_not_found >= max_not_found_threshold:
                        elapsed_time = time.time() - start_time
                        records_per_second = processed_records / elapsed_time if elapsed_time > 0 else 0
                        self.logger.info(f"Processed {processed_records} records. Average processing speed: {records_per_second:.2f} records/second.")

                except ET.ParseError as e:
                    self.logger.debug(f"Querying ILS-DI with URL: {url}")
                    self.logger.error(f"XML parsing error: {e}. Problematic record:\n{response.text}")
                    break
            else:
                self.logger.error(f"Error fetching records starting at ID {record_id} from {server['name']}: {response.status_code}")
                break

            record_id += batch_size
            time.sleep(self.pause_duration)

    def process_records_ilsdi(self, records, record_type, server):
        for record in records:
            try:
                decoded_record = html.unescape(record.text)

                # Replace invalid characters with valid XML entities
                decoded_record = decoded_record.replace("&", "&amp;")

                # Parse the MARCXML content
                record_element = ET.fromstring(decoded_record)

                # Wrap the <record> in a <collection> root element
                collection = ET.Element("{http://www.loc.gov/MARC21/slim}collection")
                collection.append(record_element)

                # Generate valid MARCXML with namespaces and declaration
                marcxml = ET.tostring(collection, encoding='utf-8', xml_declaration=True).decode('utf-8')

                # Extract source_authid for validation (from MARCXML controlfield tag 001)
                source_authid = record_element.find(".//{http://www.loc.gov/MARC21/slim}controlfield[@tag='001']")
                if source_authid is None or not source_authid.text:
                    raise ValueError("source_authid cannot be null or missing")

                # Process the record based on type
                if record_type == 'biblio':
                    parsed_data = self.parser.parse_biblio(marcxml, server['format'])
                    self.db.save_biblio(parsed_data, server['server_id'], server['format'])
                elif record_type == 'auth':
                    parsed_data = self.parser.parse_auth(marcxml, server['format'])
                    self.db.save_auth(parsed_data, server['server_id'], server['format'])
                else:
                    raise ValueError(f"Unsupported record type: {record_type}")

            except ET.ParseError as e:
                self.logger.error(f"XML parsing error: {e}. Problematic record: {decoded_record}")
            except ValueError as e:
                self.logger.error(f"Validation error: {e}. Problematic record: {decoded_record}")
            except Exception as e:
                self.logger.error(f"Error processing record: {e}. Problematic record: {decoded_record}")

# Function to clean problematic characters
def clean_text(text):
    return ''.join(char for char in text if ord(char) >= 32 or char in ['\t', '\n', '\r'])
