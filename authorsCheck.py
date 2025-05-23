import os
import json
import csv
import regex as re
import pandas as pd
import recordlinkage
from recordlinkage.preprocessing import clean
from lxml import etree
from modules.database import Database
from modules.logger import Logger
import pandas as pd
import recordlinkage
import unicodedata

# Relation words (case-insensitive matching)
RELATION_WORDS = [
    "ілюстратор", "іл.", "ил.", "перекладач", "пер.", "ред.", "ed.", "редактор"
]

# Define namespaces for parsing MARCXML
NAMESPACES = {
    "marc": "http://www.loc.gov/MARC21/slim"
}


def normalize_name_for_linkage(name):
    """
    Normalize a name for linkage by handling order variations and initials.
    Args:
        name (str): The name to normalize.
    Returns:
        str: The normalized name.
    """
    try:
        # Split the name into parts and remove extra spaces
        name_parts = [part.strip() for part in name.lower().split()]
        
        # Sort parts alphabetically to handle order variations
        name_parts_sorted = sorted(name_parts)
        
        # Rejoin the sorted parts
        normalized_name = " ".join(name_parts_sorted)
        return normalized_name
    except Exception as e:
        print(f"Error normalizing name: {name}, Error: {e}")
        return ""

def match_authors_with_recordlinkage(field_authors, marc_authors):
    """
    Match authors using recordlinkage with Jaro-Winkler similarity.

    Args:
        field_authors (list): List of authors extracted from 200$f/g, as dictionaries with 'relation' and 'author'.
        marc_authors (list): List of authors extracted from 700-712, as strings.

    Returns:
        list: List of authors with match details, including MatchedField and Matched7.
    """
    try:
        # Normalize and convert field_authors to DataFrame
        df_field = pd.DataFrame([
            {
                "index": i,
                "author": normalize_name_for_linkage(author["author"])
            }
            for i, author in enumerate(field_authors)
        ])

        # Normalize and convert marc_authors to DataFrame
        df_marc = pd.DataFrame([
            {
                "index": i,
                "author": normalize_name_for_linkage(author)
            }
            for i, author in enumerate(marc_authors)
        ])

        #print("df_field constructed:\n", df_field)
        #print("df_marc constructed:\n", df_marc)

        # Generate candidate links
        indexer = recordlinkage.Index()
        indexer.full()  # Consider all possible pairs
        candidate_links = indexer.index(df_field, df_marc)
        #print("Candidate Links:\n", candidate_links)

        # Compare authors using Jaro-Winkler similarity
        compare = recordlinkage.Compare()
        compare.string("author", "author", method="jarowinkler", threshold=0.85)
        matches = compare.compute(candidate_links, df_field, df_marc)
        #print("Matches (Jaro-Winkler):\n", matches)

        # Prepare results
        results = []
        for i, field_author in enumerate(field_authors):
            matched_author = ""
            matched_field = ""

            #print(f"Processing Field Author {i}: {field_author}")
            if i in matches.index.get_level_values(0):
                # Extract match scores for the current field_author
                match_scores = matches.xs(i, level=0, drop_level=False)
                #print(f"Match Scores for Field Author {i}:\n{match_scores}")

                # Ensure match_scores is not empty and check for valid matches
                if not match_scores.empty and match_scores[0].gt(0).any():
                    # Get the best match index (highest score)
                    best_match_idx = match_scores[0].idxmax()[1]  # Access the MARC index directly
                    matched_author = marc_authors[best_match_idx]
                    matched_field = "700-712"
                    #print(f"Best Match Index for Field Author {i}: {best_match_idx}")
                    #print(f"Best Matched MARC Author: {marc_authors[best_match_idx]}")
                #else:
                    #print(f"No valid match found for Field Author {i}.")
            #else:
                #print(f"No candidate matches found for Field Author {i}.")

            # Append the results for this field author
            results.append({
                "Author": field_author["author"],
                "Relation": field_author["relation"],
                "MatchedField": matched_field,
                "Matched7": matched_author,
            })

        return results

    except Exception as e:
        print(f"Error in match_authors_with_recordlinkage: {e}")
        return []

def process_record_with_recordlinkage(xmlrecord, bib_metadata, relation_words):
    """
    Process a single MARCXML record and find matches using recordlinkage.
    
    Args:
        xmlrecord (str): MARCXML record as a string.
        bib_metadata (dict): Metadata dictionary for the record.
        relation_words (list): List of relation words to identify authorship roles.
        
    Returns:
        list: List of dictionaries containing processed results for each author.
        str: Error message, if any.
    """
    try:
        from lxml import etree

        # Parse the MARCXML record
        root = etree.fromstring(xmlrecord.encode('utf-8'))

        # Extract field authors from 200$f and 200$g
        field_authors = []
        for datafield in root.xpath(".//marc:datafield[@tag='200']", namespaces=NAMESPACES):
            for subfield in datafield.xpath("marc:subfield[@code='f' or @code='g']", namespaces=NAMESPACES):
                subfield_code = subfield.get("code")
                text = subfield.text.strip() if subfield.text else ""
                
                # Normalize the name and extract relation
                relation, normalized_names = normalize_name(text)

                # Add each normalized name to field_authors
                for normalized_name in normalized_names:
                    field_authors.append({
                        "author": normalized_name,
                        "relation": relation,
                        "field": f"200${subfield_code}",
                        "contents": subfield.text.strip() if subfield.text else ""
                    })

        print(f"Constructed field_authors:\n{field_authors}")

        # Extract MARC authors from 700-702 (without applying normalize_name)
        marc_authors = []
        marc_author_fields = []
        for tag in ['700', '701', '702']:
            for datafield in root.xpath(f".//marc:datafield[@tag='{tag}']", namespaces=NAMESPACES):
                combined_author = " ".join(
                    subfield.text.strip() for subfield in datafield.xpath(
                        "marc:subfield[@code='a' or @code='b' or @code='c' or @code='d' or @code='g']",
                        namespaces=NAMESPACES
                    ) if subfield.text
                )
                if combined_author:
                    marc_authors.append(combined_author)  # Do not normalize for MARC authors
                    marc_author_fields.append(tag)

        print(f"Constructed marc_authors:\n{marc_authors}")

        # Match authors
        matched_results = match_authors_with_recordlinkage(
            [{"author": a["author"], "relation": a["relation"]} for a in field_authors],
            marc_authors
        )

        # Prepare results
        results = []
        for field_author, matched_result in zip(field_authors, matched_results):
            matched_field_index = (
                next((i for i, a in enumerate(marc_authors) if a == matched_result["Matched7"]), None)
            )
            matched_field = marc_author_fields[matched_field_index] if matched_field_index is not None else ""
            results.append({
                "Author": field_author["author"],
                "Field": field_author["field"],
                "FieldContents": field_author["contents"],
                "Relation": field_author["relation"],
                "Matched7": matched_result["Matched7"],
                "MatchedField": matched_field,
                **bib_metadata
            })

        return results, None

    except Exception as e:
        print(f"Error in process_record_with_recordlinkage: {e}")
        return [], str(e)


def normalize_name(name):
    """
    Normalize a name by:
    - Extracting and returning the relation keyword, if any.
    - Handling multiple names separated by commas or semicolons.
    - Reordering initials to appear after the name.
    - Using Unicode properties for matching capital and lowercase letters.

    Args:
        name (str): The raw name string.

    Returns:
        tuple: (relation, normalized_names) where:
               relation (str): Relation keyword, if any.
               normalized_names (list): List of normalized names.
    """
    # Normalize and remove special symbols
    name = unicodedata.normalize("NFKC", name)  # Normalize Unicode
    name = name.replace("\xa0", " ")           # Replace non-breaking spaces
    name = name.strip()                        # Remove leading/trailing spaces

    # Define patterns for relation keywords, initials, and names
    relation_keywords = r"(?i)(ілюстратор|іл\.|ил\.|перекладач|пер\.|ред\.|ed\.|редактор)"
    initials_pattern = r"([\p{Lu}\p{Lt}]{1,2}\.)"  # Matches one or two uppercase initials ending with a dot
    names_pattern = (r"([\p{Lu}\p{Lt}][\p{Ll}]+(?:[-\s][\p{Lu}\p{Lt}][\p{Ll}]+)*)") # Matches names starting with a capital letter

    # Compile regex with Unicode flag
    relation_regex = re.compile(relation_keywords, re.UNICODE)
    initials_regex = re.compile(initials_pattern, re.UNICODE)
    names_regex = re.compile(names_pattern, re.UNICODE)

    # Match relation keywords (if any)
    relation_match = relation_regex.search(name)
    relation = relation_match.group(0).strip() if relation_match else ""

    # Remove relation keyword from the name
    name = re.sub(relation_keywords, "", name, flags=re.UNICODE).strip()

    # Split into multiple potential names (by commas or semicolons)
    name_parts = re.split(r"[;,]", name)

    normalized_names = []
    for part in name_parts:
        # Extract initials and names
        initials = initials_regex.findall(part)
        name_match = names_regex.search(part)
        if not name_match:
            continue  # Skip if no valid name is found

        # Extract the primary name
        primary_name = name_match.group(0).strip()

        # Reorder initials to appear after the name
        normalized_name = f"{primary_name} {' '.join(initials)}".strip()
        normalized_names.append(normalized_name)

    return relation, normalized_names


def extract_marc_authors(root, tags, subfields):
    """
    Extract authors from specified MARC fields and subfields in a specific order.

    Args:
        root: XML root element.
        tags: List of MARC field tags (e.g., ['700', '701', '702']).
        subfields: List of subfield codes to extract in order (e.g., ['a', 'b', 'd', 'g', 'c', 'f']).

    Returns:
        List of concatenated author strings.
    """
    marc_authors = []
    for tag in tags:
        matches = root.xpath(f"//marc:datafield[@tag='{tag}']", namespaces=NAMESPACES)
        for match_field in matches:
            author_parts = []
            for code in subfields:  # Respect the specified order
                subfield = match_field.find(f"marc:subfield[@code='{code}']", namespaces=NAMESPACES)
                if subfield is not None and subfield.text:
                    author_parts.append(subfield.text.strip())
            if author_parts:
                marc_authors.append(" ".join(author_parts))  # Concatenate parts in the specified order
    return marc_authors


def extract_authors_from_field(field_contents, relation_words):
    """Extract authors and relations from a field."""
    authors = []
    parts = re.split(r"[;,]", field_contents)  # Split by comma or semicolon

    for part in parts:
        part = part.strip()
        relation, author = normalize_name(part)
        if author:
            authors.append({"relation": relation, "author": author})

    return authors


def fuzzy_match_author(name1, name2):
    """Check if two author names match, allowing for initials."""
    name1_parts = name1.split()
    name2_parts = name2.split()

    # Match full names or initials
    if len(name1_parts) != len(name2_parts):
        return all(part1[0] == part2[0] for part1, part2 in zip(name1_parts, name2_parts))

    return all(part1 == part2 or part1[0] == part2[0] for part1, part2 in zip(name1_parts, name2_parts))


def match_authors(field_authors, marc_authors):
    """Match authors found in 200 with those in 700 fields."""
    matches = []
    unmatched = []

    for field_author in field_authors:
        matched = False
        for marc_author in marc_authors:
            if fuzzy_match_author(field_author["author"], marc_author):
                matches.append({
                    "author": field_author["author"],
                    "relation": field_author["relation"],
                    "matched_author": marc_author,
                    "matched_field": "700"
                })
                matched = True
                break
        if not matched:
            unmatched.append({
                "author": field_author["author"],
                "relation": field_author["relation"],
                "matched_author": "",
                "matched_field": ""
            })

    return matches, unmatched


def process_record(xml_data, bib_metadata, relation_words):
    results = []
    try:
        root = etree.fromstring(xml_data.encode("utf-8"))  # Parse the XML data
    except etree.XMLSyntaxError as e:
        return [], f"Failed to parse XML: {e}. Raw XML: {xml_data}"

    # Extract 200$f and 200$g
    fields_200 = root.xpath("//marc:datafield[@tag='200']", namespaces=NAMESPACES)
    for field in fields_200:
        subfields = field.xpath("marc:subfield[@code='f' or @code='g']", namespaces=NAMESPACES)
        for subfield in subfields:
            contents = subfield.text
            if not contents:
                continue

            # Extract authors from 200
            field_authors = extract_authors_from_field(contents, relation_words)

            # Collect all authors from 700-702 and 710-712 fields
            marc_authors_700 = extract_marc_authors(root, ["700", "701", "702"], ["a", "b", "d", "g", "c", "f"])
            marc_authors_710 = extract_marc_authors(root, ["710", "711", "712"], ["a", "b", "c", "d", "e", "f", "g", "h"])
            marc_authors = marc_authors_700 + marc_authors_710

            # Log extracted authors for debugging
            print(f"Record {bib_metadata['bib_id']} Field Authors (200$f/g):", [a['author'] for a in field_authors])
            print(f"Record {bib_metadata['bib_id']} MARC Authors (700-702):", marc_authors)

            # Match authors
            matched_authors, unmatched_authors = match_authors(field_authors, marc_authors)

            # Add matched authors to results
            for match in matched_authors:
                results.append({
                    "Author": match["author"],
                    "Field": f"200${subfield.attrib['code']}",
                    "FieldContents": contents,
                    "Relation": match["relation"],
                    "Matched7": match["matched_author"],
                    "MatchedField": match["matched_field"],
                    **bib_metadata,
                })

            # Add unmatched authors to results
            for unmatched in unmatched_authors:
                results.append({
                    "Author": unmatched["author"],
                    "Field": f"200${subfield.attrib['code']}",
                    "FieldContents": contents,
                    "Relation": unmatched["relation"],
                    "Matched7": "",
                    "MatchedField": "",
                    **bib_metadata,
                })

    return results, None

def load_config():
    """Load configuration from harvester_config.json in the script's directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "harvester_config.json")
    with open(config_path, "r") as config_file:
        return json.load(config_file)


# Main function to query database and process records
def main():
    output_file = "authority_control_results.csv"

    config = load_config()
    config["logger"]["logfile"] = "authority_control.log"
    config["logger"]["name"] = "authorsCheck"

    # Initialize Logger
    logger = Logger(config["logger"])

    # Initialize Database
    db = Database(config["database"], logger)
    db.connect()

    try:
        query = "SELECT xmlrecord, bib_id, server_id, source_bibid, title, author, edition, place, publisher, date, extent, series, lang FROM bibliosource LIMIT 10"
        records = db.query_all(query)

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "Author", "Field", "FieldContents", "Relation", "Matched7", "MatchedField",
                "bib_id", "server_id", "source_bibid", "title", "author", "edition", "place",
                "publisher", "date", "extent", "series", "lang"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for record in records:
                try:
                    xmlrecord = record['xmlrecord']  # Extract the XML content
                    bib_metadata_dict = {
                        "bib_id": record['bib_id'],
                        "server_id": record['server_id'],
                        "source_bibid": record['source_bibid'],
                        "title": record['title'],
                        "author": record['author'],
                        "edition": record['edition'],
                        "place": record['place'],
                        "publisher": record['publisher'],
                        "date": record['date'],
                        "extent": record['extent'],
                        "series": record['series'],
                        "lang": record['lang']
                    }

                    # Call process_record
                    results, error = process_record_with_recordlinkage(xmlrecord, bib_metadata_dict, RELATION_WORDS)
                    if error:
                        logger.error(f"Error processing record: {error}")
                        continue  # Skip this record

                    for result in results:
                        writer.writerow(result)

                except KeyError as e:
                    logger.error(f"Missing expected key in record: {e}")
                    continue

        logger.info(f"Processing completed. Results saved to {output_file}.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
