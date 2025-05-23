import time
from modules.database import Database, load_config
from modules.logger import Logger
from SPARQLWrapper import SPARQLWrapper, JSON

def get_wikidata_ids_by_isni(isni_list):
    """
    Query Wikidata for person IDs using a batch of ISNI numbers.
    :param isni_list: List of ISNI numbers to query.
    :return: Dictionary mapping ISNI to Wikidata ID.
    """
    endpoint = "https://query.wikidata.org/sparql"
    isni_values = " ".join([f'"{isni}"' for isni in isni_list])
    query = f"""
    SELECT ?isni ?person WHERE {{
      VALUES ?isni {{ {isni_values} }}
      ?person wdt:P213 ?isni.
    }}
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        response = sparql.query().convert()
        results = response.get("results", {}).get("bindings", [])

        # Map ISNI to Wikidata IDs
        isni_to_wikidata = {result["isni"]["value"]: result["person"]["value"].split("/")[-1] for result in results}
        return isni_to_wikidata

    except Exception as e:
        print(f"Error querying Wikidata: {e}")
        return {}


# Main function to query database and process records
def main():
    """
    Fetch ISNI records where Wikidata is empty, process them in batches, and update the ISNI table with found Wikidata IDs.
    """
    # Load the database and logger
    config = load_config()
    config["logger"]["logfile"] = "log/authority_control.log"
    config["logger"]["name"] = "wikidata_getIDbyISNI"

    logger = Logger(config["logger"])
    db = Database(config["database"], logger)
    db.connect()

    # Fetch ISNI records where Wikidata is empty
    query_fetch = "SELECT ISNI FROM ISNI WHERE Wikidata IS NULL OR Wikidata = '';"
    isni_records = db.query_all(query_fetch)
    isni_list = [record["ISNI"] for record in isni_records]

    batch_size = 10  # Wikidata can process multiple ISNI numbers in a single query
    for i in range(0, len(isni_list), batch_size):
        isni_batch = isni_list[i:i + batch_size]
        try:
            # Query Wikidata for multiple ISNI IDs
            isni_to_wikidata = get_wikidata_ids_by_isni(isni_batch)

            # Update the ISNI table with the found Wikidata IDs
            for isni, wikidata_id in isni_to_wikidata.items():
                query_update = """UPDATE ISNI SET Wikidata = %s WHERE ISNI = %s;"""
                db.execute(query_update, (wikidata_id, isni))
                logger.info(f"Updated ISNI {isni} with Wikidata ID {wikidata_id}.")

            # Identify ISNI numbers without a Wikidata ID
            missing_wikidata = [isni for isni in isni_batch if isni not in isni_to_wikidata]
            for isni in missing_wikidata:
                logger.warning(f"No Wikidata ID found for ISNI {isni}.")

            # Pause to respect Wikidata query limits
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error processing ISNI batch {isni_batch}: {e}")
            time.sleep(300)  # Wait for 5 minutes in case of server restrictions

    # Process ISNI numbers from mergedISNI in batches
    query_fetch_merged = "SELECT ISNI, mergedISNI FROM ISNI WHERE Wikidata IS NULL AND mergedISNI != '';"
    merged_isni_records = db.query_all(query_fetch_merged)

    # Prepare a list of original ISNI and their associated merged ISNI numbers
    merged_isni_data = []
    for record in merged_isni_records:
        original_isni = record["ISNI"]
        merged_isni_list = record["mergedISNI"].split(",")  # Split comma-separated merged ISNI values
        merged_isni_list = [isni.strip() for isni in merged_isni_list]  # Remove extra spaces
        for merged_isni in merged_isni_list:
            merged_isni_data.append((original_isni, merged_isni))

    # Process merged ISNI in batches of 20
    batch_size = 20
    for i in range(0, len(merged_isni_data), batch_size):
        batch = merged_isni_data[i:i + batch_size]
        merged_isni_batch = [item[1] for item in batch]  # Extract merged ISNI numbers for the query
        original_to_merged = {item[1]: item[0] for item in batch}  # Map merged ISNI to original ISNI

        try:
            # Query Wikidata for the merged ISNI batch
            isni_to_wikidata = get_wikidata_ids_by_isni(merged_isni_batch)

            # Update the ISNI table with found Wikidata IDs
            for merged_isni, wikidata_id in isni_to_wikidata.items():
                original_isni = original_to_merged[merged_isni]
                query_update = """UPDATE ISNI SET Wikidata = %s WHERE ISNI = %s;"""
                db.execute(query_update, (wikidata_id, original_isni))
                logger.info(f"Updated original ISNI {original_isni} using merged ISNI {merged_isni} with Wikidata ID {wikidata_id}.")

            # Identify merged ISNI numbers without a Wikidata ID
            missing_wikidata = [isni for isni in merged_isni_batch if isni not in isni_to_wikidata]
            for isni in missing_wikidata:
                original_isni = original_to_merged[isni]
                logger.warning(f"No Wikidata ID found for merged ISNI {isni} associated with original ISNI {original_isni}.")

            # Pause for 20 seconds between batches
            time.sleep(20)

        except Exception as e:
            logger.error(f"Error processing merged ISNI batch: {merged_isni_batch}: {e}")
            time.sleep(300)  # Wait for 5 minutes in case of server restrictions


if __name__ == "__main__":
    main()
