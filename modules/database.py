import os
import json
import mysql.connector
from mysql.connector import Error

def load_config(config_file="harvester_config.json"):
    """Load configuration from a JSON file."""
    try:
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "..", config_file)  # Adjust for parent directory
        with open(config_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file {config_file} not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON configuration: {e}")


class Database:
    def __init__(self, config, logger):
        self.config = config
        self.connection = None
        self.logger = logger  # Use the logger passed from the main script

    def connect(self):
        """Establish a connection to the database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            if self.connection.is_connected():
                self.logger.info("Connected to the database.")
            else:
                self.logger.error("Failed to establish a database connection.")
        except Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise Exception(f"Error connecting to database: {e}")

    def close(self):
        """Close the database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed.")
        else:
            self.logger.warning("Attempted to close a non-existent or already closed connection.")

    def validate_connection(self):
        if not self.connection or not self.connection.is_connected():
            self.logger.warning("Database connection lost. Attempting to reconnect...")
            try:
                self.connect()  # Ensure `connect` is a method in your class that establishes the connection
                self.logger.info("Database reconnected successfully.")
            except Exception as e:
                self.logger.error(f"Failed to reconnect to the database: {e}")
                raise Exception("Database connection is not established or has been closed.")

    def query_single(self, query, params=None):
        """Execute a query and return a single result."""
        self.validate_connection()
        cursor = self.connection.cursor()
        try:
            self.logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            result = cursor.fetchone()
            self.logger.debug(f"Query result: {result}")
            return result[0] if result else None
        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise Exception(f"Error executing query: {e}")
        finally:
            cursor.close()

    def execute(self, query, params=None):
        """Execute a query without returning results."""
        self.validate_connection()
        cursor = self.connection.cursor()
        try:
            #self.logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            self.connection.commit()
            #self.logger.debug("Query executed and committed successfully.")
        except Error as e:
            self.connection.rollback()
            self.logger.error(f"Error executing query. Transaction rolled back: {e}")
            raise Exception(f"Error executing query: {e}")
        finally:
            cursor.close()

    def executemany(self, query, data):
        """
        Execute a query with multiple data entries in a single operation.

        Args:
            query (str): The SQL query to execute.
            data (list of dict): A list of dictionaries with keys matching the placeholders in the query.

        Raises:
            Exception: If an error occurs during execution.
        """
        self.validate_connection()
        cursor = self.connection.cursor()
        try:
            #self.logger.debug(f"Executing bulk query: {query}")
            cursor.executemany(query, data)
            self.connection.commit()
            #self.logger.info(f"Executed bulk query successfully for {len(data)} rows.")
        except Error as e:
            self.connection.rollback()
            self.logger.error(f"Error executing bulk query: {e}")
            raise Exception(f"Error executing bulk query: {e}")
        finally:
            cursor.close()

    def query_all(self, query, params=None):
        """Execute a query and return all results."""
        self.validate_connection()
        cursor = self.connection.cursor(dictionary=True)
        try:
            #self.logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            results = cursor.fetchall()
            #self.logger.debug(f"Query results: {results}")
            return results
        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise Exception(f"Error executing query: {e}")
        finally:
            cursor.close()

    def insert_many(self, table_name, data, batch_size=1000):
        """
        Insert multiple rows into a database table in batches.

        Args:
            table_name (str): The name of the database table.
            data (list of dict): A list of dictionaries, where keys are column names and values are the corresponding values.
            batch_size (int): The number of rows to insert in each batch.

        Raises:
            Exception: If an error occurs during execution.
        """
        if not data:
            self.logger.warning(f"No data provided to insert into table {table_name}.")
            return

        # Validate connection
        self.validate_connection()

        # Extract column names and generate query
        columns = data[0].keys()
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            # Split data into batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.logger.info(f"Inserting batch {i // batch_size + 1} with {len(batch)} rows...")
                cursor = self.connection.cursor()
                cursor.executemany(query, batch)
                self.connection.commit()
                cursor.close()
                self.logger.info(f"Batch {i // batch_size + 1} inserted successfully.")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error inserting data into {table_name}: {e}")
            raise

    def get_last_updated(self, server_id, record_type):
        """
        Retrieve the last updated timestamp for a given server and record type.
        :param server_id: ID of the server
        :param record_type: 'biblio' or 'auth' indicating the record type
        :return: The maximum last updated timestamp or None if no records exist
        """
        self.validate_connection()
        table_name = "bibliosource" if record_type == "biblio" else "authsource"
        query = f"SELECT MAX(lastupdated) FROM {table_name} WHERE server_id = %s"
        try:
            self.logger.debug(f"Executing get_last_updated query: {query} with server_id: {server_id}")
            return self.query_single(query, (server_id,))
        except Exception as e:
            self.logger.error(f"Error retrieving last updated timestamp: {e}")
            raise

    def mark_record_as_deleted(self, identifier, server_id, record_type):
        """
        Mark a record as deleted in the database.
        
        :param identifier: The unique identifier of the record
        :param server_id: The ID of the server from which the record was harvested
        :param record_type: The type of record ('biblio' or 'auth')
        """
        self.validate_connection()
        table_name = "bibliosource" if record_type == "biblio" else "authsource"
        query = f"UPDATE {table_name} SET deleted = 1 WHERE source_bibid = %s AND server_id = %s"
        try:
            self.logger.debug(f"Marking record as deleted: identifier={identifier}, server_id={server_id}")
            self.execute(query, (identifier, server_id))
            #self.logger.info(f"Record marked as deleted: {identifier}")
        except Exception as e:
            self.logger.error(f"Error marking record as deleted: {e}")
            raise

    def insert_biblio_record(self, data, server_id):
        """Insert a bibliographic record into the bibliosource table."""
        try:
            query = (
                "INSERT INTO bibliosource ("
                "server_id, source_bibid, title, author, edition, place, publisher, "
                "date, extent, series, isbn, lang, lastupdated, xmlrecord"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "title = VALUES(title), "
                "author = VALUES(author), "
                "edition = VALUES(edition), "
                "place = VALUES(place), "
                "publisher = VALUES(publisher), "
                "date = VALUES(date), "
                "extent = VALUES(extent), "
                "series = VALUES(series), "
                "isbn = VALUES(isbn), "
                "lang = VALUES(lang), "
                "lastupdated = VALUES(lastupdated), "
                "xmlrecord = VALUES(xmlrecord)"
            )
            params = (
                server_id, data['source_bibid'], data['title'], data['author'], data['edition'],
                data['place'], data['publisher'], data['date'], data['extent'], data['series'],
                data['isbn'], data['lang'], data['lastupdated'], data['xmlrecord']
            )
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            return cursor.lastrowid  # Retrieve the last inserted ID
        except Exception as e:
            raise Exception(f"Error inserting bibliographic record: {e}")
        finally:
            cursor.close()

    def insert_biblio_marc21_record(self, original_marcxml, bib_id, server_id):
        """Insert the original MARC21 bibliographic record into the bibliosource21 table."""
        try:
            query = (
                "INSERT INTO bibliosource21 ("
                "bib_id, server_id, source_bibid, title, lastupdated, xmlrecord"
                ") SELECT %s, %s, source_bibid, title, lastupdated, %s "
                "FROM bibliosource WHERE bib_id = %s "
                "ON DUPLICATE KEY UPDATE "
                "title = VALUES(title), "
                "lastupdated = VALUES(lastupdated), "
                "xmlrecord = VALUES(xmlrecord)"
            )
            params = (bib_id, server_id, original_marcxml, bib_id)
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            raise Exception(f"Error inserting MARC21 bibliographic record: {e}")
        finally:
            cursor.close()

    def insert_auth_record(self, data, server_id):
        """Insert or update an authority record in the authsource table."""
        try:
            query = (
                "INSERT INTO authsource ("
                "server_id, source_authid, authtype, lang, title, isni, lastupdated, xmlrecord, deleted"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0) "
                "ON DUPLICATE KEY UPDATE "
                "authtype = VALUES(authtype), "
                "lang = VALUES(lang), "
                "title = VALUES(title), "
                "isni = VALUES(isni), "
                "lastupdated = VALUES(lastupdated), "
                "xmlrecord = VALUES(xmlrecord), "
                "deleted = 0"
            )
            params = (
                server_id, data['source_authid'], data['authtype'], data['lang'], data['title'],
                data['isni'], data['lastupdated'], data['xmlrecord']
            )
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            return cursor.lastrowid  # Retrieve the last inserted ID
        except Exception as e:
            raise Exception(f"Error inserting or updating authority record: {e}")
        finally:
            cursor.close()

    def insert_auth_marc21_record(self, original_marcxml, auth_id, server_id):
        """Insert or update the original MARC21 authority record in the authsource21 table."""
        try:
            query = (
                "INSERT INTO authsource21 ("
                "auth_id, server_id, source_authid, title, lastupdated, xmlrecord"
                ") SELECT %s, %s, source_authid, title, lastupdated, %s "
                "FROM authsource WHERE auth_id = %s "
                "ON DUPLICATE KEY UPDATE "
                "title = VALUES(title), "
                "lastupdated = VALUES(lastupdated), "
                "xmlrecord = VALUES(xmlrecord)"
            )
            params = (auth_id, server_id, original_marcxml, auth_id)
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            raise Exception(f"Error inserting or updating MARC21 authority record: {e}")
        finally:
            cursor.close()

    def save_biblio(self, data, server_id, format_type):
        """Save bibliographic records to the database."""
        try:
            # Insert the converted MARCXML into bibliosource
            bib_id = self.insert_biblio_record(data, server_id)

            # Insert the original MARC21 record into bibliosource21 if applicable
            if format_type == 'MARC21' and data.get('original_marcxml'):
                if bib_id:
                    self.insert_biblio_marc21_record(data['original_marcxml'], bib_id, server_id)
                else:
                    raise ValueError("Failed to retrieve bib_id for MARC21 record insertion.")
        except Exception as e:
            raise Exception(f"Error saving bibliographic record: {e}")

    def save_auth(self, data, server_id, format_type):
        """Save authority records to the database."""
        try:            
            # Insert the converted MARCXML into authsource
            auth_id = self.insert_auth_record(data, server_id)
            #self.logger.debug(f"Auth record saved with auth_id: {auth_id}")
            
            # Insert the original MARC21 record into authsource21 if applicable
            if format_type == 'MARC21' and data.get('original_marcxml'):
                if auth_id:
                    self.insert_auth_marc21_record(data['original_marcxml'], auth_id, server_id)
                else:
                    raise ValueError("Failed to retrieve auth_id for MARC21 record insertion.")
        except Exception as e:
            self.logger.error(f"Error in save_auth: {e}")
            raise Exception(f"Error saving authority record: {e}")

    def insert_isni_record(self, data):
        """Insert or update a record in the ISNI table."""
        try:
            query = (
                "INSERT INTO ISNI ("
                "ISNI, mergedISNI, Name, Wikidata, VIAF, marcxml, basicxml"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "mergedISNI = VALUES(mergedISNI), "
                "Name = VALUES(Name), "
                "Wikidata = VALUES(Wikidata), "
                "VIAF = VALUES(VIAF), "
                "marcxml = VALUES(marcxml), "
                "basicxml = VALUES(basicxml)"
            )
            params = (
                data["ISNI"], data["mergedISNI"], data["Name"],
                data["Wikidata"], data["VIAF"], data["marcxml"], data["basicxml"]
            )
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            self.logger.info(f"ISNI record inserted/updated for {data['ISNI']}")
        except Exception as e:
            self.logger.error(f"Error inserting/updating ISNI record: {e}")
            raise
        finally:
            cursor.close()

    def insert_wikidata_record(self, data):
        """Insert or update a record in the ISNI table."""
        try:
            # Check if all required keys are in the data
            required_keys = ["wikidata_id", "nameEN", "nameUK", "nameRU", "marcxml", "json"]
            missing_keys = [key for key in required_keys if key not in data]
            if missing_keys:
                self.logger.error(f"Missing keys in data: {missing_keys}")
                raise KeyError(f"Missing keys in data: {missing_keys}")
            query = """
            INSERT INTO wikidata (wikidata_id, nameEN, nameUK, nameRU, marcxml, json)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE nameEN=VALUES(nameEN), nameUK=VALUES(nameUK), nameRU=VALUES(nameRU), marcxml=VALUES(marcxml), json=VALUES(json)
            """
            params = (
                data["wikidata_id"], data["nameEN"], data["nameUK"],
                data["nameRU"], data["marcxml"], data["json"]
            )
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            self.logger.info(f"Wikidata record inserted/updated for {data['wikidata_id']}")
        except Exception as e:
            self.logger.error(f"Error inserting/updating Wikidata record: {e}")
            raise
        finally:
            cursor.close()

    def insert_authsource_normalized(self, normalized_data, auth_id):
        """
        Insert normalized data into the authsource_normalized table.
        Before inserting, remove all previous records with the same auth_id.

        Args:
            normalized_data (list): List of dictionaries containing the normalized data.
            auth_id (int): The auth_id to remove prior records for.
        """
        try:
            # Validate the connection
            self.validate_connection()

            # Remove previous records for the same auth_id
            delete_query = "DELETE FROM authsource_normalized WHERE auth_id = %s"
            self.execute(delete_query, (auth_id,))

            # Prepare and execute bulk insert query
            if normalized_data:
                insert_query = """
                    INSERT INTO authsource_normalized (
                        auth_id, server_id, ISNI, field, lang, entryname,
                        initials, given_name, dates, roman, full_name
                    ) VALUES (
                        %(auth_id)s, %(server_id)s, %(ISNI)s, %(field)s, %(lang)s, %(entryname)s,
                        %(initials)s, %(given_name)s, %(dates)s, %(roman)s, %(full_name)s
                    )
                """
                self.executemany(insert_query, normalized_data)
                #self.logger.info(f"Inserted {len(normalized_data)} records into authsource_normalized for auth_id {auth_id}.")
            else:
                self.logger.warning(f"No normalized data to insert for auth_id {auth_id}.")
        except Exception as e:
            self.logger.error(f"Error inserting normalized data for auth_id {auth_id}: {e}")
            raise
