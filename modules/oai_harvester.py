from modules.marcxml_parser import MarcXmlParser
from modules.logger import Logger
import time
from lxml import etree
from sickle import Sickle
from sickle.models import Record
from datetime import datetime

class OaiHarvester:
    def __init__(self, db, parser_config, logger, pause_duration=1, batch_size=10):
        self.logger = logger
        self.db = db
        self.parser = MarcXmlParser(parser_config)
        self.pause_duration = pause_duration
        self.batch_size = batch_size

    def harvest(self, server, record_type):
        """Harvest records from an OAI-PMH server."""
        try:
            self.logger.info(f"Starting harvest for server: {server['name']}, type: {record_type}")
            sickle = Sickle(server['uri'])

            # Determine the start date for harvesting
            last_updated = self.db.get_last_updated(server['server_id'], record_type)
            if last_updated:
                from_date = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")  # Convert to ISO 8601 format
            else:
                from_date = None

            # Fetch records using OAI-PMH
            try:
                self.logger.debug(f"Fetching records with from_date: {from_date}, server URI: {server['uri']}")
                records = sickle.ListRecords(
                    **{'metadataPrefix': 'marc21',
                    'from': from_date if from_date else None,
                    'until': server.get('enddate', None)
                })
            except Exception as fetch_error:
                self.logger.error(f"Failed to fetch records: {fetch_error}")
                response = sickle.response  # Capture the raw response
                self.logger.debug(f"Raw server response: {response}")
                return

            record_count = 0
            for record in records:
                try:
                    if hasattr(record, 'deleted') and record.deleted:
                        self._handle_deleted_record(record, server, record_type)
                    else:
                        self._process_record(record, server, record_type)
                    record_count += 1
                    
                    # Apply pause after processing batch_size records
                    if record_count % self.batch_size == 0:
                        self.logger.info(f"Processed {record_count} records. Pausing for {self.pause_duration} seconds.")
                        time.sleep(self.pause_duration)

                except Exception as e:
                    self.logger.error(f"Error processing record: {e}")
                    self.logger.debug(f"Full record details: {record.__dict__}")

        except Exception as e:
            self.logger.error(f"Error during harvesting from server {server['name']}: {e}")

    def _handle_deleted_record(self, record, server, record_type):
        """Handle a record marked as deleted."""
        try:
            self.db.mark_record_as_deleted(record.header.identifier.split(":")[-1], server['server_id'], record_type)
            self.logger.info(f"Marked record as deleted: {record.header.identifier}")
        except Exception as e:
            self.logger.error(f"Error marking record as deleted: {e}")

    def _process_record(self, record, server, record_type):
        """Process and store a record based on its type."""
        try:
            # Extract raw MARCXML from the record object
            if hasattr(record, 'xml'):
                raw_marcxml = etree.tostring(record.xml, encoding="unicode")
            else:
                self.logger.warning(f"Record does not contain XML data: {record.header.identifier}")
                return

            # Ensure raw_marcxml contains only the MARCXML <record> tag
            root = etree.fromstring(raw_marcxml)
            marcxml_element = root.find(".//{http://www.loc.gov/MARC21/slim}record")
            if marcxml_element is None:
                self.logger.warning(f"No MARCXML <record> found in metadata: {record.header.identifier}")
                return

            # Wrap the MARCXML <record> with <collection> and add namespaces
            marcxml = f"""<?xml version="1.0" encoding="UTF-8"?>
            <collection xmlns="http://www.loc.gov/MARC21/slim" 
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                        xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
                {etree.tostring(marcxml_element, encoding="unicode")}
            </collection>"""

            # Normalize lastupdated from OAI header datestamp
            lastupdated = None
            if hasattr(record.header, 'datestamp'):
                lastupdated = record.header.datestamp
                try:
                    # Handle both date-only and full ISO 8601 formats
                    if "T" in lastupdated:
                        lastupdated = datetime.strptime(lastupdated, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        lastupdated = datetime.strptime(lastupdated, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    self.logger.warning(f"Failed to normalize datetime value: {lastupdated}. Error: {e}")
                    lastupdated = None
                    
            # Pass cleaned MARCXML and normalized lastupdated to parsed data
            if record_type == 'biblio':
                parsed_data = self.parser.parse_biblio(marcxml, server['format'])
                if lastupdated:
                    parsed_data["lastupdated"] = lastupdated
                self.db.save_biblio(parsed_data, server['server_id'], server['format'])
            elif record_type == 'auth':
                parsed_data = self.parser.parse_auth(marcxml, server['format'])
                if lastupdated:
                    parsed_data["lastupdated"] = lastupdated
                self.db.save_auth(parsed_data, server['server_id'], server['format'])
            else:
                raise ValueError(f"Unsupported record type: {record_type}")
        except Exception as e:
            self.logger.error(f"Error processing record: {e}")