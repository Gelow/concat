import unicodedata
import re
from lxml import etree
from modules.homoglyph import convert_to_latin, convert_to_cyrillic
from modules.database import Database, load_config
from modules.logger import Logger

# Initialize logger
config = load_config()
config["logger"]["logfile"] = "log/authority_deduplication.log"
config["logger"]["name"] = "auth_normalize_table"

logger = Logger(config["logger"])

db = Database(config["database"], logger)
db.connect()

def is_valid_isni(isni):
    return bool(re.match(r'^\d{15}[\dX]$', isni))

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



# Define character conversion mappings
char_conversion = {
    "Æ": "AE", "æ": "ae",
    "Œ": "OE", "œ": "oe",
    "Đ": "D", "đ": "d",
    "Ð": "D", "ð": "d",
    "ı": "i",  # Lowercase Turkish i → I
    #"ł": "L", "Ł": "L",  # Polish L
    #"ƛ": "L",  # Script small L
    "Ơ": "O", "ơ": "o",  # O Hook
    "Ư": "U", "ư": "u",  # U Hook
    "Ø": "O", "ø": "o",  # Scandinavian O
    "Þ": "TH", "þ": "th",  # Icelandic Thorn
    "ß": "SS",  # Eszett symbol
    "©": "", "℗": "", "®": "",  # Remove copyright, patent, sound recording marks
    "™": "", "°": "", "±": "", "‰": ""  # Remove degree, plus/minus, per mille sign
}

# Define punctuation processing rules
punctuation_rules = {
    "!": " ", "\"": " ", "'": "", "(": " ", ")": " ",
    "-": "-", "[": "", "]": "", "{": " ", "}": " ",
    "<": " ", ">": " ", ";": " ", ":": " ", ".": ".",
    "?": " ", "¿": " ", "¡": " ", ",": " ", "/": " ",
    "\\": " ", "@": "@", "&": "&", "*": " ", "|": " ",
    "%": " ", "=": " ", "+": "+", "−": "-", "ℤ": " ",
    "×": "x", "÷": "/", "‘": " ", "’": " ", "‛": " ",
    "“": " ", "”": " ", "„": " ", "‧": " ", "·": " ",
}

# Define characters to delete
chars_to_delete = set("ʾʿ·")

# Define regex patterns for superscripts and subscripts
superscript_map = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹", "0123456789")
subscript_map = str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789")

# Unicode combining marks for modifying diacritics to remove
MODIFYING_DIACRITICS = {
    "\u0301",  # Acute
    "\u0306",  # Breve
    "\u0310",  # Candrabindu
    "\u0327",  # Cedilla
    "\u030A",  # Circle above (Å)
    "\u0325",  # Circle below
    "\u0302",  # Circumflex
    "\u0323",  # Dot below
    "\u030B",  # Double acute
    "\u0360",  # Double tilde (first half)
    "\u0361",  # Double tilde (second half)
    "\u0333",  # Double underscore
    "\u0300",  # Grave
    "\u030C",  # Hacek
    "\u0315",  # High comma centered
    "\u0313",  # High comma off center
    "\u0312",  # Left hook
    "\u0362",  # Ligature (first half)
    "\u0363",  # Ligature (second half)
    "\u0304",  # Macron
    "\u0374",  # Pseudo question mark
    "\u0328",  # Right hook, ogonek
    "\u0326",  # Right cedilla
    "\u0308",  # Umlaut, diaeresis
    "\u0332",  # Underscore
    "\u0953",  # Upadhmaniya
    "\u0303",  # Tilde
}

def remove_selected_diacritics(text):
    """
    Removes only specific modifying diacritics while keeping essential diacritics (e.g., Й, Ё).
    """
    decomposed = unicodedata.normalize("NFD", text)
    filtered = "".join(ch for ch in decomposed if ch not in MODIFYING_DIACRITICS)
    return unicodedata.normalize("NFC", filtered)  # Recompose the characters



def clean_text(text):
    if not isinstance(text, str) or not text.strip():
        return ""

    # Normalize Unicode
    #text = unicodedata.normalize("NFKD", text)

    # Remove diacritics
    #text = remove_selected_diacritics(text)

    # Convert superscripts and subscripts to normal numbers
    text = text.translate(superscript_map)
    text = text.translate(subscript_map)

    # Replace specific characters based on the mapping dictionary
    text = "".join(char_conversion.get(char, char) for char in text)

    # Remove specified characters
    text = "".join(char for char in text if char not in chars_to_delete)

    # Process punctuation based on the rules
    text = "".join(punctuation_rules.get(char, char) for char in text)

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\x00-\x1F\x7F]+", "", text)  # Remove control characters

    return text

def extract_lang(subfield_8, field_100a, field_num):
    if subfield_8 and len(subfield_8) == 3:
        return subfield_8
    elif field_num in ("200", "400") and field_100a:
        return field_100a[9:12]
    return None

import re

def normalize_dates(date_text):
    """
    Cleans and normalizes date fields, allowing formats like YYYY, YYYY-YYYY, and YYYY-.
    Removes non-numeric characters except dashes.
    """
    if not isinstance(date_text, str) or not date_text.strip():
        return ""

    # Remove unwanted characters (keep digits, dash, and question mark for uncertainty)
    date_text = re.sub(r"[^0-9\-?]", "", date_text)

    # Allowable formats: YYYY, YYYY-YYYY, YYYY-, YYYY?
    if re.match(r"^\d{4}(-\d{4})?$", date_text):  # YYYY or YYYY-YYYY
        return date_text
    elif re.match(r"^\d{4}-?$", date_text):  # YYYY- (open-ended)
        return date_text
    elif re.match(r"^\d{4}\?$", date_text):  # YYYY? (uncertain)
        return date_text[:-1]  # Remove the question mark
    else:
        return ""  # If no valid format, return an empty string

def normalize_isni(isni):
    if isni:
        isni = re.sub(r"[^\dXx]", "", isni)
        return isni if is_valid_isni(isni) else ""
    return ""

def process_language(entryname, initials, given_name, lang):
    if re.search(r"[a-zA-Z]", entryname) or re.search(r"[a-zA-Z]", initials) or re.search(r"[a-zA-Z]", given_name):
        entryname = convert_to_latin(entryname)
        initials = convert_to_latin(initials)
        given_name = convert_to_latin(given_name)
        if lang not in ("eng", "pol", "fra"):
            return entryname, initials, given_name, "eng"
    elif re.search(r"[\u0400-\u04FF]", entryname) or re.search(r"[\u0400-\u04FF]", initials) or re.search(r"[\u0400-\u04FF]", given_name):
        entryname = convert_to_cyrillic(entryname)
        initials = convert_to_cyrillic(initials)
        given_name = convert_to_cyrillic(given_name)
        if lang not in ("ukr", "rus"):
            if re.search(r"['IiІіЇїЄє]", entryname):
                return entryname, initials, given_name, "ukr"
            if re.search(r"[ЪъЭэЫыЁё]", entryname):
                return entryname, initials, given_name, "rus"
            else:
                logger.warning(f"Unrecognized Cyrillic text without language: {entryname}")
                return entryname, initials, given_name, ""
    return entryname, initials, given_name, lang

def parse_marcxml(xmlrecord, fields=None):
    """
    Parses a MARCXML record and extracts specified fields and subfields.

    Args:
        xmlrecord (str): The MARCXML record as a string.
        fields (list): List of field tags (e.g., ["010", "100", "200", "400", "700"]).

    Returns:
        dict: A dictionary where keys are field tags and values are lists of subfield dictionaries.
    """
    result = {}
    try:
        root = etree.fromstring(xmlrecord.encode('utf-8'))
        namespaces = {'marc': 'http://www.loc.gov/MARC21/slim'}
        for datafield in root.xpath(".//marc:datafield", namespaces=namespaces):
            tag = datafield.attrib.get("tag")
            if fields and tag not in fields:
                continue

            subfields = {}
            for subfield in datafield.xpath("marc:subfield", namespaces=namespaces):
                code = subfield.attrib.get("code")
                value = subfield.text.strip() if subfield.text else ""
                subfields[f"${code}"] = value

            if tag not in result:
                result[tag] = []
            result[tag].append(subfields)
    except etree.XMLSyntaxError as e:
        logger.error(f"XML parsing error: {e}. XML content: {xmlrecord[:200]}...")
    return result

# Main processing function
def normalize_authority_data():
    query = "SELECT auth_id, server_id, xmlrecord FROM authsource WHERE authtype = 200" # and server_id = 7"
    data = db.query_all(query)

    for record in data:
        auth_id = record["auth_id"]
        server_id = record["server_id"]
        xmlrecord = record["xmlrecord"]

        if not xmlrecord or not xmlrecord.strip():
            logger.error(f"Empty XML record for auth_id {auth_id}. Skipping.")
            continue

        try:
            etree.fromstring(xmlrecord.encode('utf-8'))
        except etree.XMLSyntaxError as e:
            logger.error(f"Invalid XML record for auth_id {auth_id}: {e}. Skipping.")
            continue

        # Parse fields
        fields = parse_marcxml(xmlrecord, fields=["010", "100", "200", "400", "700"])
        #logger.debug(f"Parsed fields for auth_id {auth_id}: {fields}")

        normalized_data = []
        for field_num in ["200", "400", "700"]:
            field_data = fields.get(field_num, [])
            #logger.debug(f"Processing field {field_num} for auth_id {auth_id}. Field data: {field_data}")

            for subfield_set in field_data:
                #logger.debug(f"Subfield set: {subfield_set}")

                entryname = clean_text(subfield_set.get("$a", ""))
                subfield_g = clean_text(subfield_set.get("$g", ""))

                if server_id == 7 and entryname in subfield_g:
                    subfield_g = re.sub(rf"^{re.escape(entryname)},?\s*", "", subfield_g)

                # Ensure space after dot if not followed by space or end
                subfield_g = re.sub(r"\.(?!\s|$)", ". ", subfield_g)
                subfield_g = re.sub(r"[,\\-]+$", "", subfield_g)
                initials = convert_to_initials(subfield_g) if subfield_g else clean_text(subfield_set.get("$b", ""))

                dates = normalize_dates(subfield_set.get("$f", ""))
                #logger.debug(f"Extracted dates: {dates}")

                roman = clean_text(subfield_set.get("$d", ""))
                #logger.debug(f"Extracted roman: {roman}")

                subfield_8 = subfield_set.get("$8", "")
                field_100a = fields.get("100", [{}])[0].get("$a", "")
                lang = extract_lang(subfield_8, field_100a, field_num)
                #logger.debug(f"Final lang: {lang}")

                entryname, initials, subfield_g, lang = process_language(entryname, initials, subfield_g, lang)
                #logger.debug(f"Processed entryname: {entryname}, initials: {initials}, given_name: {subfield_g}, lang: {lang}")

                full_name = entryname + (" " + subfield_g if subfield_g else " " + initials)
                #logger.debug(f"Constructed full_name: {full_name}")

                isni = normalize_isni(clean_text(fields.get("010", [{}])[0].get("$a", "")))
                #logger.debug(f"Extracted ISNI: {isni}")

                normalized_data.append({
                    "auth_id": auth_id,
                    "server_id": server_id,
                    "ISNI": isni,
                    "field": int(field_num),
                    "lang": lang,
                    "entryname": entryname,
                    "initials": initials,
                    "given_name": subfield_g,
                    "dates": dates,
                    "roman": roman,
                    "full_name": full_name
                })

        #logger.debug(f"Normalized data for auth_id {auth_id}: {normalized_data}")
        db.insert_authsource_normalized(normalized_data, auth_id)

if __name__ == "__main__":
    normalize_authority_data()
    logger.info("Authority deduplication normalization complete.")
