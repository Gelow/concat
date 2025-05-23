import os
import csv
import json
from lxml import etree
from modules.database import Database
from modules.logger import Logger
from itertools import chain

def extract_field_values(root, tag, subfields):
    """Extract and combine subfield values for a given MARC field tag."""
    values = []
    for field in root.xpath(f".//marc:datafield[@tag='{tag}']", namespaces={"marc": "http://www.loc.gov/MARC21/slim"}):
        subfield_values = list(chain.from_iterable(
            field.xpath(f"marc:subfield[@code='{code}']/text()", namespaces={"marc": "http://www.loc.gov/MARC21/slim"})
            for code in subfields
        ))
        combined = " ".join(subfield_values).strip()
        if combined:
            values.append(combined)
    return values

def extract_auth_no(root):
    """Extract the AuthNo value from subfield $3 or $9 in any of the relevant fields."""
    for tag in ["700", "710", "500"]:
        for field in root.xpath(f".//marc:datafield[@tag='{tag}']", namespaces={"marc": "http://www.loc.gov/MARC21/slim"}):
            for code in ["3", "9"]:
                value = field.xpath(f"marc:subfield[@code='{code}']/text()", namespaces={"marc": "http://www.loc.gov/MARC21/slim"})
                if value:
                    return value[0].strip()
    return ""

def extract_record_type(root):
    """Extract the record type from the 8th character of the leader."""
    try:
        ns = {"marc": "http://www.loc.gov/MARC21/slim"}
        record = root.find("marc:record", namespaces=ns)
        if record is None:
            return "Missing record"
        leader = record.find("marc:leader", namespaces=ns)
        if leader is not None and leader.text:
            if len(leader.text) > 7:
                return leader.text[7]  # Extract the 8th character
            else:
                return "Invalid leader length"
        else:
            return "Missing leader"
    except Exception as e:
        print(f"Error extracting record type: {e}")
        return "Error"

def process_records(records, output_file, logger):
    """Process records and write extracted data to CSV."""
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "PrAuthor", "PrCorpAuth", "UnifTitle", "RecordType", "AuthNo", "bib_id", "server_id", "source_bibid",
            "title", "author", "edition", "place", "publisher", "date", "extent", "series", "lang"
        ])

        for record in records:
            try:
                xmlrecord = record['xmlrecord']
                root = etree.fromstring(xmlrecord.encode("utf-8"))

                pr_author = " ".join(extract_field_values(root, "700", ["a", "b", "d", "g", "c", "f"])) or ""
                pr_corp_auth = " ".join(extract_field_values(root, "710", ["a", "b", "c", "d", "e", "f", "g", "h"])) or ""
                unif_title = " ".join(extract_field_values(root, "500", ["a", "b", "h", "i", "k", "l", "m"])) or ""

                record_type = extract_record_type(root)
                if record_type in ["Missing leader", "Invalid leader length", "Error"]:
                    logger.warning(f"Invalid or missing leader for bib_id {record['bib_id']}")
                auth_no = extract_auth_no(root)

                if len(root.xpath(".//marc:datafield[@tag='700']", namespaces={"marc": "http://www.loc.gov/MARC21/slim"})) > 1:
                    logger.warning(f"Multiple 700 fields found for bib_id {record['bib_id']}")
                if len(root.xpath(".//marc:datafield[@tag='710']", namespaces={"marc": "http://www.loc.gov/MARC21/slim"})) > 1:
                    logger.warning(f"Multiple 710 fields found for bib_id {record['bib_id']}")

                writer.writerow([
                    str(pr_author), str(pr_corp_auth), str(unif_title), str(record_type), str(auth_no),
                    str(record['bib_id']), str(record['server_id']), str(record['source_bibid']),
                    str(record['title']), str(record['author']), str(record['edition']), str(record['place']),
                    str(record['publisher']), str(record['date']), str(record['extent']), str(record['series']), str(record['lang'])
                ])

            except Exception as e:
                logger.error(f"Error processing record with bib_id {record['bib_id']}: {e}")

def main():
    config = load_config()

    # Initialize Logger
    logger = Logger(config["logger"])

    # Initialize Database
    db = Database(config["database"], logger)
    db.connect()

    query = "SELECT bib_id, server_id, source_bibid, title, author, edition, place, publisher, date, extent, series, lang, xmlrecord FROM bibliosource"

    try:
        records = db.query_all(query)
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return

    output_file = "author_extracted_data.csv"
    process_records(records, output_file, logger)

    logger.info(f"Data extraction complete. Output saved to {output_file}.")

def load_config():
    """Load configuration from harvester_config.json in the script's directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "harvester_config.json")
    with open(config_path, "r") as config_file:
        return json.load(config_file)

if __name__ == "__main__":
    main()
