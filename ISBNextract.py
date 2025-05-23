import os
import csv
from lxml import etree
from modules.database import Database, load_config
from modules.logger import Logger
from isbnlib import canonical, to_isbn13, to_isbn10, is_isbn13, is_isbn10, mask

def extract_isbn_from_marcxml(xmlrecord):
    """Extract ISBNs from MARCXML in specified fields."""
    isbns = []
    try:
        if not xmlrecord or not xmlrecord.strip().startswith("<"):
            raise ValueError("Invalid or empty XML record")
        
        # Ensure the input is in bytes format
        if isinstance(xmlrecord, str):
            xmlrecord = xmlrecord.encode("utf-8")

        root = etree.fromstring(xmlrecord)
        ns = {"marc": "http://www.loc.gov/MARC21/slim"}

        # XPath queries with namespaces
        for tag, subfield_code in [("010", "a"), ("010", "z")]:
            for subfield in root.xpath(f".//marc:datafield[@tag='{tag}']/marc:subfield[@code='{subfield_code}']", namespaces=ns):
                text = subfield.text.strip()
                if '(' in text:
                    text = text.split('(')[0].strip()  # Remove content in parentheses
                text = text.replace('х', 'X').replace('Х', 'X')  # Replace Cyrillic Х with Latin X
                isbns.append((text, f"{tag}${subfield_code}"))
        for datafield in root.xpath(".//marc:datafield[substring(@tag, 1, 1) = '4']", namespaces=ns):
            for subfield in datafield.xpath("marc:subfield[@code='y']", namespaces=ns):
                # Split multiple ISBNs separated by commas/spaces and clean extra info
                isbn_candidates = [item.split('(')[0].strip().replace('х', 'X').replace('Х', 'X') for item in subfield.text.split(',')]
                for isbn in isbn_candidates:
                    isbns.append((isbn, f"{datafield.get('tag')}$y"))
    except Exception as e:
        print(f"Error processing MARCXML: {e}")
    return isbns

def parse_publisher_ranges(xml_file_path):
    """Parse the ISBN range message XML file and build publisher lookup."""
    try:
        root = etree.parse(xml_file_path).getroot()
        #print(f"Root tag: {root.tag}")
        
        publisher_ranges = {}

        # Find all Group elements
        groups = root.xpath("//Group")
        #print(f"Found {len(groups)} groups in XML")
        
        for group in groups:
            prefix = group.find("Prefix")
            agency = group.find("Agency")
            rules = group.find("Rules")

            if prefix is not None and agency is not None and rules is not None:
                prefix_text = prefix.text
                agency_text = agency.text
                for rule in rules.findall("Rule"):
                    range_text = rule.find("Range").text
                    publisher_ranges[(prefix_text, range_text)] = agency_text

        #print(f"Parsed {len(publisher_ranges)} publisher ranges")
        return publisher_ranges
    except Exception as e:
        print(f"Error parsing publisher ranges: {e}")
        return {}

def find_publisher_info(isbn13, publisher_ranges):
    """Find the agency name and range for a given ISBN-13."""
    try:
        for (prefix, range_text), agency in publisher_ranges.items():
            if isbn13.startswith(prefix.replace("-", "")):
                range_start, range_end = map(int, range_text.split("-"))
                publisher_id = int(isbn13[len(prefix.replace("-", "")):len(prefix.replace("-", "")) + 7])  # Extract publisher ID
                if range_start <= publisher_id <= range_end:
                    return agency, range_text
        return "Unknown Agency", "Unknown Range"
    except Exception as e:
        print(f"Error finding publisher info: {e}")
        return "Unknown Agency", "Unknown Range"

def diagnose_isbn_issue(isbn):
    """Diagnose why an ISBN is invalid."""
    allowed_chars = set("0123456789Xx -")
    if any(char not in allowed_chars for char in isbn):
        return "Invalid characters"
    isbn_cleaned = ''.join(char for char in isbn if char.isdigit() or char.upper() == 'X')
    if len(isbn_cleaned) not in [10, 13]:
        return "Incorrect length"
    if len(isbn_cleaned) == 10 and not is_isbn10(isbn_cleaned):
        return "Invalid checksum"
    if len(isbn_cleaned) == 13 and not is_isbn13(isbn_cleaned):
        return "Invalid checksum"
    return "Valid"

def process_isbn(isbn):
    """Validate, convert, and mask an ISBN."""
    try:
        canonical_isbn = canonical(isbn.replace('Х', 'X'))
        isbn13 = to_isbn13(canonical_isbn)
        masked_isbn = mask(canonical_isbn)
        return canonical_isbn, isbn13, masked_isbn
    except Exception as e:
        #print(f"Error processing ISBN {isbn}: {e}")
        return isbn, None, None

def main():
    config = load_config()
    config["logger"]["logfile"] = "log/isbn.log"
    config["logger"]["name"] = "ISBNextract"

    # Initialize Logger
    logger = Logger(config["logger"])
    
    # Initialize Database with error handling
    db = Database(config["database"], logger)
    db.connect()

    xml_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "export_rangemessage.xml")
    publisher_ranges = parse_publisher_ranges(xml_file_path)

    query = "SELECT bib_id, server_id, source_bibid, title, author, edition, place, publisher, date, extent, series, lang, xmlrecord FROM bibliosource"
    
    try:
        records = db.query_all(query)
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return

    output_file = "isbn_extracted_data.csv"
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Original ISBN", "Datafield", "Masked ISBN", "Canonical ISBN",
            "bib_id", "server_id", "source_bibid", "title", "author", "edition", "place", "publisher", "date", "extent", "series", "lang",
            "Validity", "Agency", "Range"
        ])

        for record in records:
            bib_id = record['bib_id']
            server_id = record['server_id']
            source_bibid = record['source_bibid']
            title = record['title']
            author = record['author']
            edition = record['edition']
            place = record['place']
            publisher = record['publisher']
            date = record['date']
            extent = record['extent']
            series = record['series']
            lang = record['lang']
            xmlrecord = record['xmlrecord']
            try:
                logger.info(f"Processing record with bib_id {bib_id}")
                isbns = extract_isbn_from_marcxml(xmlrecord)
            except Exception as e:
                logger.error(f"Error processing record with bib_id {bib_id}: {e}")
                
                continue  # Skip invalid records

            for original_isbn, datafield in isbns:
                try:
                    canonical_isbn, isbn13, masked_isbn = process_isbn(original_isbn)
                    if is_isbn13(isbn13) or is_isbn10(canonical_isbn):
                        validity = "Valid"
                        agency, range_text = find_publisher_info(isbn13, publisher_ranges)
                    else:
                        validity = diagnose_isbn_issue(original_isbn)
                        agency, range_text = "Unknown", "Unknown"
                except Exception as e:
                    logger.error(f"Error processing ISBN {original_isbn}: {e}")
                    validity = "Unknown error"
                    canonical_isbn, isbn13, masked_isbn, agency, range_text = None, None, None, "Unknown", "Unknown"

                writer.writerow([
                    original_isbn, datafield, masked_isbn, isbn13,
                    bib_id, server_id, source_bibid, title, author, edition, place, publisher, date, extent, series, lang,
                    validity, agency, range_text
                ])

    logger.info(f"Data extraction complete. Output saved to {output_file}.")

if __name__ == "__main__":
    main()
