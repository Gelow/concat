import requests
from modules.database import Database, load_config
from modules.logger import Logger
import time

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


# Main function to query database and process records
def main():
    """
    Fetch ISNI records where Wikidata is empty, process them, and update the ISNI table with found Wikidata IDs.
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

    for record in isni_records:
        isni = record["ISNI"]
        try:
            # Query Wikidata for the person ID
            wikidata_id = get_wikidata_id_by_isni(isni)

            if wikidata_id:
                # Use query_single to update the ISNI table
                query_update = """UPDATE ISNI SET Wikidata = %s WHERE ISNI = %s;"""
                db.execute(query_update, (wikidata_id, isni))
                logger.info(f"Updated ISNI {isni} with Wikidata ID {wikidata_id}.")
            else:
                logger.warning(f"No Wikidata ID found for ISNI {isni}.")

            # Pause to respect Wikidata query limits
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error processing ISNI {isni}: {e}")

if __name__ == "__main__":
    main()
