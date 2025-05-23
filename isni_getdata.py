import requests
import time
import re
from lxml import etree
from modules.database import Database, load_config
from modules.logger import Logger

# Function to check if an ISNI is valid
def is_valid_isni(isni):
    return bool(re.match(r'^\d{15}[\dX]$', isni))

def query_isni_server(isni_batch):
    """
    Query the ISNI server for a batch of ISNI numbers.
    :param isni_batch: List of up to 10 ISNI numbers.
    :return: SRU XML response as bytes.
    """
    base_url = "https://isni.oclc.org/sru/"
    query = " OR ".join([f'"{isni}"' for isni in isni_batch])
    params = {
        "operation": "searchRetrieve",
        "recordSchema": "isni-b",
        "query": f"pica.isn = ({query})",
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.content  # Return raw bytes instead of decoding
    else:
        raise Exception(f"ISNI server query failed with status code {response.status_code}.")

def process_isni_batch(isni_batch, db, logger):
    """
    Process a batch of ISNI numbers by querying the ISNI server and handling each record.
    :param isni_batch: List of ISNI numbers to query.
    :param db: Database object for saving the data.
    :param logger: Logger object for logging.
    """
    try:
        # Query the ISNI server
        xml_response = query_isni_server(isni_batch)
        namespaces = {"srw": "http://www.loc.gov/zing/srw/"}
        tree = etree.fromstring(xml_response)

        # Process each record in the response
        records = tree.findall(".//srw:record", namespaces=namespaces)
        if not records:
            logger.warning(f"No records found in the response for batch: {isni_batch}")
            return

        processed_count = 0
        for record in records:
            record_data = record.find(".//srw:recordData", namespaces=namespaces)

            # Log the raw content of <srw:recordData> for debugging
            #logger.debug(f"Raw <srw:recordData> content: {etree.tostring(record_data, encoding='unicode', pretty_print=True)}")

            # Ensure record_data exists and contains meaningful content
            if record_data is not None and record_data.find(".//isniUnformatted") is not None: #and etree.tostring(record_data).strip():
                isni = record_data.findtext(".//isniUnformatted")
                logger.info(f"Processing ISNI {isni}")
                processed_data = process_isni_response(isni, record_data, logger)  # Pass the element directly
                if processed_data:
                    # Save processed data to the database
                    db.insert_isni_record(processed_data)
                    processed_count += 1
            else:
                logger.warning(f"<srw:recordData> is empty or invalid for record in batch: {isni_batch}")
                logger.debug(f"Raw <srw:recordData> content: {etree.tostring(record_data, encoding='unicode', pretty_print=True)}")
                time.sleep(3600)  # Wait for 5 minutes in case of server restrictions

        # Pause for 30 seconds between queries
        time.sleep(30)
        return processed_count

    except Exception as e:
        logger.error(f"Error processing ISNI batch {isni_batch}: {e}")
        time.sleep(300)  # Wait for 5 minutes in case of server restrictions
        return 0
    

def get_wikidata_id_by_isni(isni):
    """
    Query Wikidata for a person ID using ISNI.
    :param isni: The ISNI to query.
    :return: Wikidata ID (e.g., Q12345) or None if not found.
    """
    endpoint = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?person WHERE {{
      ?person wdt:P213 "{isni}".
    }}
    """
    headers = {"Accept": "application/json"}
    response = requests.get(endpoint, params={"query": query}, headers=headers)

    # Print the server's raw response for debugging
    if response.status_code == 200:
        data = response.json()
        bindings = data.get("results", {}).get("bindings", [])
        if bindings:
            return bindings[0]["person"]["value"].split("/")[-1]  # Extract the ID (e.g., Q12345)
    return None

def convert_to_initials(forename):
    """
    Convert a forename to initials, retaining hyphens and spaces between names.
    Example: "Jean-Paul" -> "J.-P."; "Jean Paul" -> "J. P."
    """
    if not forename:
        return ""
    words = forename.split()
    initials = []
    for word in words:
        parts = word.split("-")  # Split hyphen-separated parts
        hyphenated_initials = "-".join([part[0] + "." for part in parts])
        initials.append(hyphenated_initials)
    return " ".join(initials)

def process_isni_response(isni, record_tree, logger):
    """
    Process the ISNI XML response to extract relevant fields.
    :param isni: The ISNI being processed.
    :param xml_response: The XML response from the ISNI server.
    :param db: Database object for saving the data.
    :param logger: Logger object for logging.
    :return: Processed data dictionary or None.
    """
    # Ensure ISNI is extracted from <isniUnformatted> in the record
    isni = record_tree.findtext(".//isniUnformatted", default=isni)
    
    # Save record_data as basicxml
    basicxml = etree.tostring(record_tree, encoding="unicode", pretty_print=True)

    # Initialize variables
    merged_isni_list = [elem.text for elem in record_tree.findall(".//mergedISNI") if elem.text]
    merged_isni = ",".join(merged_isni_list)
    name_field = None
    wikidata = None  # Ensure wikidata is always initialized
    viaf_uris = []

    # Extract Name with priority LC > LCNACO > NLR
    subsource_identifier = None
    for personal_name in record_tree.findall(".//identity/personOrFiction/personalName"):
        sources = [source.text for source in personal_name.findall("source")]

        if "WKP" in sources:  # Check for Wikidata in subsourceIdentifier
            subsource_identifier = personal_name.find("subsourceIdentifier")
            if subsource_identifier is not None and subsource_identifier.text:
                wikidata = subsource_identifier.text

        if any(source in ["LC", "LCNACO", "NLR"] for source in sources):
            forename = personal_name.find("forename")
            surname = personal_name.find("surname")
            name_title = personal_name.find("nameTitle")
            marc_date = personal_name.find("marcDate")
            numeration = personal_name.find("numeration")

            forename_text = forename.text if forename is not None else ""
            surname_text = surname.text if surname is not None else ""
            name_title_text = name_title.text if name_title is not None else ""
            marc_date_text = marc_date.text if marc_date is not None else ""
            numeration_text = numeration.text if numeration is not None else ""

            initials = convert_to_initials(forename_text)

            script_code = "ca" if re.search(r'[\u0400-\u04FF]', surname_text) else "ba"
            lang_code = "rus" if "NLR" in sources else "eng" if "LCNACO" in sources else ""

            name_field = f"{surname_text} {forename_text} {numeration_text} {name_title_text} {marc_date_text}".strip()
            break  # Stop at the highest priority match

    # Extract LCNACO subsourceIdentifier from <sources>
    if subsource_identifier is None:
        for source in record_tree.findall(".//sources"):
            code_of_source = source.find("codeOfSource")
            source_identifier = source.find("sourceIdentifier")
            if code_of_source is not None and code_of_source.text == "LCNACO" and source_identifier is not None:
                subsource_identifier = source_identifier.text
                break

    if not name_field:
        logger.warning(f"Name for ISNI {isni} does not have LC, LCNACO, or NLR sources.")

    # Extract VIAF
    viaf_uris = [uri.text.split("/")[-1] for uri in record_tree.findall(".//externalInformation/URI") if "viaf.org" in uri.text]
    if not viaf_uris:
        viaf_uris = [uri.text.split("/")[-1] for uri in record_tree.findall(".//reference/URI") if "viaf.org" in uri.text]
    viaf = " ".join(viaf_uris)

    # Generate MARCXML with fields 010, 035, and 700
    root = etree.Element("record", xmlns="http://www.loc.gov/MARC21/slim")

    # 010 Field (ISNI)
    datafield_010 = etree.SubElement(root, "datafield", tag="010", ind1=" ", ind2=" ")
    subfield_a = etree.SubElement(datafield_010, "subfield", code="a")
    subfield_a.text = isni

    # 035 Field (VIAF and Wikidata)
    for viaf in viaf_uris:
        datafield_035_viaf = etree.SubElement(root, "datafield", tag="035", ind1=" ", ind2=" ")
        subfield_a = etree.SubElement(datafield_035_viaf, "subfield", code="a")
        subfield_a.text = f"(viaf){viaf}"

    if wikidata:  # Only add Wikidata if it exists
        datafield_035_wikidata = etree.SubElement(root, "datafield", tag="035", ind1=" ", ind2=" ")
        subfield_a = etree.SubElement(datafield_035_wikidata, "subfield", code="a")
        subfield_a.text = f"(wikidata){wikidata}"

    # 700 Fields (Personal Names)
    if name_field:
        datafield_700 = etree.SubElement(root, "datafield", tag="700", ind1=" ", ind2="1" if forename_text else "0")

        # Add non-empty subfields conditionally
        if surname_text:
            subfield_a = etree.SubElement(datafield_700, "subfield", code="a")
            subfield_a.text = surname_text

        if initials:
            subfield_b = etree.SubElement(datafield_700, "subfield", code="b")
            subfield_b.text = initials

        if forename_text:
            subfield_g = etree.SubElement(datafield_700, "subfield", code="g")
            subfield_g.text = forename_text

        if numeration_text:
            subfield_d = etree.SubElement(datafield_700, "subfield", code="d")
            subfield_d.text = numeration_text

        if marc_date_text:
            subfield_f = etree.SubElement(datafield_700, "subfield", code="f")
            subfield_f.text = marc_date_text

        if subsource_identifier:
            subfield_3 = etree.SubElement(datafield_700, "subfield", code="3")
            subfield_3.text = subsource_identifier

        if script_code:
            subfield_7 = etree.SubElement(datafield_700, "subfield", code="7")
            subfield_7.text = script_code

        if lang_code:
            subfield_8 = etree.SubElement(datafield_700, "subfield", code="8")
            subfield_8.text = lang_code

    marcxml = etree.tostring(root, pretty_print=True, encoding="unicode")

    # Check for warnings
    if isni in merged_isni_list:
        logger.warning(f"Queried ISNI {isni} is found in mergedISNI list: {merged_isni_list}")

    return {
        "ISNI": isni,
        "mergedISNI": merged_isni,
        "Name": name_field,
        "Wikidata": wikidata,
        "VIAF": viaf,
        "basicxml": basicxml,
        "marcxml": marcxml
    }


# Main function to query database and process records
def main():
    config = load_config()
    config["logger"]["logfile"] = "log/authority_control.log"
    config["logger"]["name"] = "ISNI_getdata"

    # Initialize Logger
    logger = Logger(config["logger"])

    # Initialize Database
    db = Database(config["database"], logger)
    db.connect()

    # Fetch unique ISNI numbers
    query = "SELECT DISTINCT REGEXP_REPLACE(isni, '[^0-9X]', '') AS isni FROM authsource WHERE isni IS NOT NULL AND REGEXP_REPLACE(isni, '[^0-9X]', '') NOT IN (SELECT ISNI FROM ISNI) AND NOT EXISTS ( SELECT 1 FROM ISNI WHERE FIND_IN_SET(authsource.isni, mergedISNI) );"
    #query = "SELECT DISTINCT REGEXP_REPLACE(isni, '[^0-9X]', '') AS isni FROM authsource WHERE isni ='0000000456119713';"
    all_isni_records = db.query_all(query)
    isni_list = [record["isni"] for record in all_isni_records if is_valid_isni(record["isni"])]

    processed_records = 0
    batch_size = 10
    for i in range(0, len(isni_list), batch_size):
        isni_batch = isni_list[i:i + batch_size]
        #logger.info(f"Processing ISNI batch: {isni_batch}")
        processed_count = process_isni_batch(isni_batch, db, logger)
        processed_records += processed_count

        # Pause for 6 hours after 400 records
        if processed_records >= 400:
            logger.info("Processed 400 records. Pausing for 6 hours.")
            time.sleep(6 * 3600)  # 6 hours in seconds
            processed_records = 0

if __name__ == "__main__":
    main()
