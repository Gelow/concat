import argparse
from modules.oai_harvester import OaiHarvester
#from modules.sru_harvester import SruHarvester  # Placeholder for SRU Harvester
from modules.kohaapi_harvester import KohaAPIHarvester  # Placeholder for Koha API Harvester
from modules.logger import Logger
from modules.database import Database, load_config

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Harvester for multiple protocols")
    parser.add_argument("--serverid", type=int, help="Server ID to harvest")
    parser.add_argument("--host", help="Server host to harvest")
    parser.add_argument("--startdate", help="Harvest data last modified since date (YYYY-MM-DD)")
    parser.add_argument("--enddate", help="Harvest data last modified until date (YYYY-MM-DD)")
    parser.add_argument("--logfile", help="Path to the log file")
    parser.add_argument("--logconsole", action="store_true", help="Enable logging to console")
    parser.add_argument("--batchsize", type=int, default=10, help="Number of records to process before pausing")
    parser.add_argument("--pause", type=int, default=1, help="Pause duration (in seconds) between server responses")
    args = parser.parse_args()

    # Load configuration
    config = load_config()

    db_config = config["database"]
    parser_config = config["parser"]
    logger_config = config["logger"]

    if args.logfile:
        logger_config["logfile"] = args.logfile
    logger_config["console"] = args.logconsole

    # Initialize Logger and Database
    logger = Logger(logger_config)
    logger.info("Starting the harvester")
    db = Database(db_config, logger)  # Pass the logger to the Database
    db.connect()

    try:
        # Determine which servers to harvest
        if args.serverid or args.host:
            # Fetch specific servers based on command-line arguments
            server_query = (
                "SELECT * FROM servers WHERE "
                + ("server_id = %s" if args.serverid else "host = %s")
            )
            server_param = args.serverid if args.serverid else args.host
            servers = db.query_all(server_query, (server_param,))
        else:
            # Fetch all servers if no specific arguments are provided
            servers = db.query_all("SELECT * FROM servers WHERE enabled = 1")

        if not servers:
            logger.error("No servers found matching the specified criteria.")
            return

        # Dispatch harvesting based on the protocol
        for server in servers:
            protocol = server["servertype"].lower()
            record_type = server["recordtype"]
            logger.info(f"Harvesting {record_type} records from server: {server['name']} using {protocol}")

            if protocol == "oai":
                harvester = OaiHarvester(db, parser_config, logger, pause_duration=args.pause, batch_size=args.batchsize)
#            elif protocol == "sru":
#                harvester = SruHarvester(db, parser_config, logger, pause_duration=args.pause, batch_size=args.batchsize)
            elif protocol == "kohaapi":
                harvester = KohaAPIHarvester(db, parser_config, logger, pause_duration=args.pause, batch_size=args.batchsize)
            else:
                logger.error(f"Unsupported protocol: {protocol}")
                continue

            # Harvest records
            harvester.harvest(server, record_type)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        db.close()
        logger.info("Harvester completed.")

if __name__ == "__main__":
    main()
