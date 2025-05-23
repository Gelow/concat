import time
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from lxml.etree import Element, SubElement, tostring
from modules.database import Database, load_config
from modules.logger import Logger

# Initialize logger
config = load_config()
config["logger"]["logfile"] = "log/wikidata.log"
config["logger"]["name"] = "wikidata_getdata"

logger = Logger(config["logger"])

# SPARQL endpoint
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

def fetch_wikidata_data(ids):
    """
    Fetch data from Wikidata for the given IDs.
    :param ids: List of Wikidata IDs.
    :return: JSON response.
    """
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    ids_clause = " ".join([f"wd:{id}" for id in ids])
    query = f"""
    SELECT ?item ?itemLabel ?fullNameEN ?fullNameUK ?fullNameRU
           (GROUP_CONCAT(DISTINCT ?altNameEN; separator="|") AS ?altNamesEN)
           (GROUP_CONCAT(DISTINCT ?altNameUK; separator="|") AS ?altNamesUK)
           (GROUP_CONCAT(DISTINCT ?altNameRU; separator="|") AS ?altNamesRU)
           ?birthDate ?deathDate ?isni ?viaf ?loc ?bnf ?nlr ?gnd ?nativeLangCode ?countryCode ?sexOrGenderLabel
           ?firstNameENLabel ?lastNameENLabel ?firstNameUKLabel ?lastNameUKLabel ?firstNameRULabel ?lastNameRULabel
           (GROUP_CONCAT(DISTINCT ?ukWiki; separator="|") AS ?ukWikis)
           (GROUP_CONCAT(DISTINCT ?enWiki; separator="|") AS ?enWikis)
           (GROUP_CONCAT(DISTINCT ?ruWiki; separator="|") AS ?ruWikis)
    WHERE {{
      VALUES ?item {{ {ids_clause} }}
      OPTIONAL {{ ?item rdfs:label ?fullNameEN FILTER(LANG(?fullNameEN) = "en") }}
      OPTIONAL {{ ?item rdfs:label ?fullNameUK FILTER(LANG(?fullNameUK) = "uk") }}
      OPTIONAL {{ ?item rdfs:label ?fullNameRU FILTER(LANG(?fullNameRU) = "ru") }}
      OPTIONAL {{ ?item skos:altLabel ?altNameEN FILTER(LANG(?altNameEN) = "en") }}
      OPTIONAL {{ ?item skos:altLabel ?altNameUK FILTER(LANG(?altNameUK) = "uk") }}
      OPTIONAL {{ ?item skos:altLabel ?altNameRU FILTER(LANG(?altNameRU) = "ru") }}
      OPTIONAL {{ ?item wdt:P569 ?birthDate }}
      OPTIONAL {{ ?item wdt:P570 ?deathDate }}
      OPTIONAL {{ ?item wdt:P213 ?isni }}
      OPTIONAL {{ ?item wdt:P214 ?viaf }}
      OPTIONAL {{ ?item wdt:P244 ?loc }}
      OPTIONAL {{ ?item wdt:P268 ?bnf }}
      OPTIONAL {{ ?item wdt:P3183 ?nlr }}
      OPTIONAL {{ ?item wdt:P227 ?gnd }}
      OPTIONAL {{ ?item wdt:P103 ?nativeLang . ?nativeLang wdt:P220 ?nativeLangCode }}
      OPTIONAL {{ ?item wdt:P27 ?country . ?country wdt:P297 ?countryCode }}
      OPTIONAL {{ ?item wdt:P21 ?sexOrGender . ?sexOrGender rdfs:label ?sexOrGenderLabel FILTER(LANG(?sexOrGenderLabel) = "en") }}
      OPTIONAL {{ ?item wdt:P735 ?firstNameEN . ?firstNameEN rdfs:label ?firstNameENLabel FILTER(LANG(?firstNameENLabel) = "en") }}
      OPTIONAL {{ ?item wdt:P734 ?lastNameEN . ?lastNameEN rdfs:label ?lastNameENLabel FILTER(LANG(?lastNameENLabel) = "en") }}
      OPTIONAL {{ ?item wdt:P735 ?firstNameUK . ?firstNameUK rdfs:label ?firstNameUKLabel FILTER(LANG(?firstNameUKLabel) = "uk") }}
      OPTIONAL {{ ?item wdt:P734 ?lastNameUK . ?lastNameUK rdfs:label ?lastNameUKLabel FILTER(LANG(?lastNameUKLabel) = "uk") }}
      OPTIONAL {{ ?item wdt:P735 ?firstNameRU . ?firstNameRU rdfs:label ?firstNameRULabel FILTER(LANG(?firstNameRULabel) = "ru") }}
      OPTIONAL {{ ?item wdt:P734 ?lastNameRU . ?lastNameRU rdfs:label ?lastNameRULabel FILTER(LANG(?lastNameRULabel) = "ru") }}
      OPTIONAL {{ ?ukWiki schema:about ?item ; schema:inLanguage "uk" ; schema:isPartOf <https://uk.wikipedia.org/> }}
      OPTIONAL {{ ?enWiki schema:about ?item ; schema:inLanguage "en" ; schema:isPartOf <https://en.wikipedia.org/> }}
      OPTIONAL {{ ?ruWiki schema:about ?item ; schema:inLanguage "ru" ; schema:isPartOf <https://ru.wikipedia.org/> }}
    }}
    GROUP BY ?item ?itemLabel ?fullNameEN ?fullNameUK ?fullNameRU
             ?birthDate ?deathDate ?isni ?viaf ?loc ?bnf ?nlr ?gnd ?nativeLangCode ?countryCode ?sexOrGenderLabel
             ?firstNameENLabel ?lastNameENLabel ?firstNameUKLabel ?lastNameUKLabel ?firstNameRULabel ?lastNameRULabel
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def create_unimarc_record(data):
    """
    Create a UNIMARC record for an author based on the Wikidata data using lxml.
    :param data: Dictionary with author data.
    :return: MARCXML string.
    """
    record = Element("record", attrib={"xmlns": "http://www.loc.gov/MARC21/slim"})

    # 010$a - ISNI
    if "isni" in data and data["isni"]:
        field_010 = SubElement(record, "datafield", tag="010", ind1=" ", ind2=" ")
        SubElement(field_010, "subfield", code="a").text = data["isni"]

    # 035$a - Other identifiers
    for key, source in {
        "viaf": "(VIAF)",
        "loc": "(US-dlc)",
        "bnf": "(FR-PaBFM)",
        "nlr": "(RU-SpRNB)",
        "gnd": "(DE-101)"
    }.items():
        if key in data and data[key]:
            field_035 = SubElement(record, "datafield", tag="035", ind1=" ", ind2=" ")
            SubElement(field_035, "subfield", code="a").text = f"{source}{data[key]}"

    # Add Wikidata ID to 035
    if "wikidata_id" in data and data["wikidata_id"]:
        field_035 = SubElement(record, "datafield", tag="035", ind1=" ", ind2=" ")
        SubElement(field_035, "subfield", code="a").text = f"{data['wikidata_id']} (Wikidata)"


    # 101$a - Language code
    if "nativeLangCode" in data and data["nativeLangCode"]:
        field_101 = SubElement(record, "datafield", tag="101", ind1=" ", ind2=" ")
        SubElement(field_101, "subfield", code="a").text = data["nativeLangCode"]

    # 102$a - Country code
    if "countryCode" in data and data["countryCode"]:
        field_102 = SubElement(record, "datafield", tag="102", ind1=" ", ind2=" ")
        SubElement(field_102, "subfield", code="a").text = data["countryCode"]

    # 120$a - Gender
    if "sexOrGender" in data and data["sexOrGender"]:
        gender_map = {"female": "a", "male": "b"}
        gender_char = gender_map.get(data["sexOrGender"].lower(), "c")
        field_120 = SubElement(record, "datafield", tag="120", ind1=" ", ind2=" ")
        SubElement(field_120, "subfield", code="a").text = gender_char + "a"

    # 400 - Alternative names
    for lang, lang_code in [("altNameEN", "eng"), ("altNameUK", "ukr"), ("altNameRU", "rus")]:
        if lang in data and data[lang]:
            for alt_name in data[lang]:
                field_400 = SubElement(record, "datafield", tag="400", ind1=" ", ind2="0")
                SubElement(field_400, "subfield", code="a").text = alt_name
                SubElement(field_400, "subfield", code="8").text = lang_code

    # 700 - Main name and dates
    for lang, lang_code, first_name_key, last_name_key, full_name_key in [
        ("EN", "eng", "firstNameEN", "lastNameEN", "nameEN"),
        ("UK", "ukr", "firstNameUK", "lastNameUK", "nameUK"),
        ("RU", "rus", "firstNameRU", "lastNameRU", "nameRU")
    ]:
        if full_name_key in data and data[full_name_key]:
            if data.get(first_name_key) and data.get(last_name_key):
                # If both family name and first name are present
                field_700 = SubElement(record, "datafield", tag="700", ind1=" ", ind2="1")
                SubElement(field_700, "subfield", code="a").text = data[last_name_key]
                SubElement(field_700, "subfield", code="g").text = data[first_name_key]
                SubElement(field_700, "subfield", code="b").text = convert_to_initials(data[first_name_key])
            else:
                # If only full name is present
                field_700 = SubElement(record, "datafield", tag="700", ind1=" ", ind2="0")
                SubElement(field_700, "subfield", code="a").text = data[full_name_key]

            # Add birth and death years if available
            if "birthDate" in data or "deathDate" in data:
                birth_year = data.get("birthDate", "")[:4] if data.get("birthDate") else ""
                death_year = data.get("deathDate", "")[:4] if data.get("deathDate") else ""
                SubElement(field_700, "subfield", code="f").text = f"{birth_year}-{death_year}"

            # Add language code
            SubElement(field_700, "subfield", code="8").text = lang_code

    # 856 - Wikipedia links
    for url_key, source_text in [
        ("ukWiki", "Вікіпедія"),
        ("enWiki", "Wikipedia"),
        ("ruWiki", "Википедия")
    ]:
        if url_key in data and data[url_key]:
            urls = [url for url in data[url_key] if url]  # Filter out empty URLs
            for url in urls:
                field_856 = SubElement(record, "datafield", tag="856", ind1="4", ind2="0")
                SubElement(field_856, "subfield", code="u").text = url
                SubElement(field_856, "subfield", code="2").text = source_text

    # Convert XML tree to a string
    return tostring(record, pretty_print=True, encoding="unicode")



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

def main():
    """
    Main function to fetch data, process it, and save it to the database.
    """
    logger.info("Fetching data from database...")

    # Connect to the database
    db = Database(config["database"], logger)
    db.connect()

    try:
        # Get Wikidata IDs from the ISNI table that are not already in the wikidata table
        query = (
            "SELECT DISTINCT wikidata FROM ISNI "
            "WHERE Wikidata IS NOT NULL AND Wikidata NOT IN (SELECT wikidata_id FROM wikidata)"
        )
        cursor = db.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        wikidata_ids = [row[0] for row in results]

        # Process IDs in batches of 10
        batch_size = 10
        for i in range(0, len(wikidata_ids), batch_size):
            batch = wikidata_ids[i:i + batch_size]
            logger.info(f"Processing batch: {batch}")

            # Fetch data from Wikidata
            results = fetch_wikidata_data(batch)

            for result in results['results']['bindings']:
                # Extract data and split concatenated fields into lists
                data = {
                    "wikidata_id": result["item"]["value"].split("/")[-1],
                    "nameEN": result.get("fullNameEN", {}).get("value"),
                    "nameUK": result.get("fullNameUK", {}).get("value"),
                    "nameRU": result.get("fullNameRU", {}).get("value"),
                    "altNameEN": result.get("altNamesEN", {}).get("value", "").split("|"),
                    "altNameUK": result.get("altNamesUK", {}).get("value", "").split("|"),
                    "altNameRU": result.get("altNamesRU", {}).get("value", "").split("|"),
                    "birthDate": result.get("birthDate", {}).get("value"),
                    "deathDate": result.get("deathDate", {}).get("value"),
                    "isni": result.get("isni", {}).get("value"),
                    "viaf": result.get("viaf", {}).get("value"),
                    "loc": result.get("loc", {}).get("value"),
                    "bnf": result.get("bnf", {}).get("value"),
                    "nlr": result.get("nlr", {}).get("value"),
                    "gnd": result.get("gnd", {}).get("value"),
                    "nativeLangCode": result.get("nativeLangCode", {}).get("value"),
                    "countryCode": result.get("countryCode", {}).get("value"),
                    "sexOrGender": result.get("sexOrGenderLabel", {}).get("value"),
                    "firstNameEN": result.get("firstNameENLabel", {}).get("value"),
                    "lastNameEN": result.get("lastNameENLabel", {}).get("value"),
                    "firstNameUK": result.get("firstNameUKLabel", {}).get("value"),
                    "lastNameUK": result.get("lastNameUKLabel", {}).get("value"),
                    "firstNameRU": result.get("firstNameRULabel", {}).get("value"),
                    "lastNameRU": result.get("lastNameRULabel", {}).get("value"),
                    "ukWiki": result.get("ukWikis", {}).get("value", "").split("|"),
                    "enWiki": result.get("enWikis", {}).get("value", "").split("|"),
                    "ruWiki": result.get("ruWikis", {}).get("value", "").split("|"),
                }

                # Generate MARCXML
                data["marcxml"] = create_unimarc_record(data)
                data["json"] = json.dumps(result, ensure_ascii=False)

                # Log the transformed data
                #logger.debug(f"Transformed data: {data}")

                # Save to database
                db.insert_wikidata_record(data)

            # Pause for 20 seconds between batches
            time.sleep(30)

    except Exception as e:
        logger.error(f"Error processing batches: {e}")
        time.sleep(300)  # Wait for 5 minutes in case of server restrictions
        raise
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    main()
