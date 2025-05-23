import xml.etree.ElementTree as ET
from datetime import datetime
from lxml import etree
import os

class MarcXmlParser:
    NS = {'marc': 'http://www.loc.gov/MARC21/slim'}

    def __init__(self, config):
        self.config = config

    def parse_biblio(self, marcxml, server_format):
        """Parse MARCXML bibliographic record into structured dictionaries."""
        original_marcxml = marcxml
        xmlrecord = marcxml

        if server_format == "MARC21":
            # Convert MARC21 to UNIMARC
            xmlrecord = self._convert_marc21_to_unimarc(marcxml, self.config.get("biblioXSL"))

        parsed_data = self.extract_biblio_data(xmlrecord)
        return {
            "xmlrecord": xmlrecord,  # The converted MARCXML (for bibliosource)
            "original_marcxml": original_marcxml if server_format == "MARC21" else None,
            **parsed_data,
        }
    
    def extract_biblio_data(self, marcxml):
        """Extract bibliographic record data from a MARCXML record."""
        try:
            tree = ET.ElementTree(ET.fromstring(marcxml))

            # Extract and process required data fields
            title = self._extract_subfields(tree, './/marc:datafield[@tag="200"]', self.NS, ['a', 'c', 'v', 'h', 'i'])
            author = self._extract_subfields(tree, './/marc:datafield[@tag="200"]', self.NS, ['f'])
            edition = self._extract_subfields(tree, './/marc:datafield[@tag="205"]', self.NS)
            place = self._extract_subfields(tree, './/marc:datafield[@tag="210"]', self.NS, ['a'])
            publisher = self._extract_subfields(tree, './/marc:datafield[@tag="210"]', self.NS, ['c'])
            date = self._extract_subfields(tree, './/marc:datafield[@tag="210"]', self.NS, ['d'])
            extent = self._extract_subfields(tree, './/marc:datafield[@tag="215"]', self.NS)
            series = self._extract_subfields(tree, './/marc:datafield[@tag="225"]', self.NS)
            isbn = self._extract_subfields(tree, './/marc:datafield[@tag="010"]', self.NS, ['a'])
            lang = self._extract_subfields(tree, './/marc:datafield[@tag="101"]', self.NS, ['a'])

            # Convert `lastupdated` to a timestamp
            lastupdated_raw = self._extract_field(tree, './/marc:controlfield[@tag="005"]', self.NS)
            lastupdated = self._convert_to_timestamp(lastupdated_raw)

            data = {
                'source_bibid': self._extract_field(tree, './/marc:controlfield[@tag="001"]', self.NS),
                'title': title,
                'author': author,
                'edition': edition,
                'place': place,
                'publisher': publisher,
                'date': date,
                'extent': extent,
                'series': series,
                'isbn': isbn,
                'lang': lang,
                'lastupdated': lastupdated,
            }

            return data
        except Exception as e:
            raise ValueError(f"Error extracting bibliographic data: {e}")

    def parse_auth(self, marcxml, server_format):
        """Parse MARCXML authority record into a structured dictionary."""
        if not isinstance(marcxml, str):
            raise ValueError("marcxml must be a string.")

        original_marcxml = marcxml
        if server_format == "MARC21":
            marcxml = self._convert_marc21_to_unimarc(marcxml, self.config.get("authXSL"))

        data = self.extract_auth_data(marcxml)
        data['original_marcxml'] = original_marcxml if server_format == "MARC21" else None
        data['xmlrecord'] = marcxml  # Add the MARCXML to the returned data
        return data

    def extract_auth_data(self, marcxml):
        """Extract authority record data from a MARCXML record."""
        try:
            tree = ET.ElementTree(ET.fromstring(marcxml))

            # Extract and process required data fields
            source_authid = self._extract_field(tree, './/marc:controlfield[@tag="001"]', self.NS)
            
            # Replace substring-based logic with Python filtering
            authtype_elements = tree.findall('.//marc:datafield', self.NS)
            authtype = next((elem.get('tag') for elem in authtype_elements if elem.get('tag', '').startswith('2')), None)
            
            # Extract title based on authtype-specific subfields
            if authtype == "200":
                subfields = ["a", "b", "d", "g", "c", "f"]
            elif authtype == "210":
                subfields = ["a", "b", "c", "d", "e", "f", "g", "h"]
            else:
                subfields = None  # Extract all subfields

            title = self._extract_subfields(tree, f'.//marc:datafield[@tag="{authtype}"]', self.NS, subfields) if authtype else None

            # Extract lang from field 100, characters 9-12
            field_100 = self._extract_subfields(tree, './/marc:datafield[@tag="100"]', self.NS,"a")
            lang = field_100[9:12] if field_100 and len(field_100) > 12 else None
            
            isni = self._extract_subfields(tree, './/marc:datafield[@tag="010"]', self.NS, ['a'])

            lastupdated_raw = self._extract_field(tree, './/marc:controlfield[@tag="005"]', self.NS)
            lastupdated = self._convert_to_timestamp(lastupdated_raw)

            data = {
                'source_authid': source_authid,
                'authtype': authtype,
                'lang': lang,
                'title': title,
                'isni' : isni,
                'lastupdated': lastupdated,
            }

            return data
        except Exception as e:
            raise ValueError(f"Error extracting authority data: {e}")

    def _extract_field(self, tree, xpath, ns):
        """Extract the text content of a single field based on the given XPath."""
        element = tree.find(xpath, ns)
        return element.text.strip() if element is not None else None

    def _extract_subfields(self, tree, xpath, ns, subfield_codes=None):
        """Extract concatenated text content of subfields from a datafield."""
        elements = tree.findall(xpath, ns)
        if not elements:
            return None

        subfields = []
        for element in elements:
            if subfield_codes:
                for code in subfield_codes:
                    subfield = element.find(f"marc:subfield[@code='{code}']", ns)
                    if subfield is not None and subfield.text:
                        subfields.append(subfield.text.strip())
            else:
                subfields.extend(
                    subfield.text.strip()
                    for subfield in element.findall("marc:subfield", ns)
                    if subfield.text
                )

        return " ".join(subfields) if subfields else None

    def _convert_to_timestamp(self, raw_date):
        """Convert a MARC 005 field value to a timestamp."""
        try:
            # Handle MARC 005 format: YYYYMMDDHHMMSS.S
            if raw_date:
                # Remove any fractional seconds (e.g., '.1')
                raw_date = raw_date.split('.')[0]
                return datetime.strptime(raw_date, "%Y%m%d%H%M%S")
            return None
        except ValueError as e:
            self.logger.warning(f"Failed to normalize datetime value: {raw_date}. Error: {e}")
            return None

    def _convert_marc21_to_unimarc(self, marcxml, xsl_path):
        """Convert MARC21 XML to UNIMARC XML using an XSLT transformation."""
        try:
            if not xsl_path or not os.path.exists(xsl_path):
                raise FileNotFoundError(f"XSL file not found: {xsl_path}")

            xslt = etree.parse(xsl_path)
            transform = etree.XSLT(xslt)
            xml_doc = etree.fromstring(marcxml.encode('utf-8'))
            transformed_doc = transform(xml_doc)

            return str(transformed_doc)
        except Exception as e:
            raise ValueError(f"Error converting MARC21 to UNIMARC: {e}")
