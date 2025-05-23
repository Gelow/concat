import logging

class Logger:
    def __init__(self, config):
        """Initialize the logger with configurations."""
        self.logger = logging.getLogger(config.get("name", "Harvester"))
        self.logger.setLevel(config.get("level", logging.INFO))

        # Log to file if a path is provided
        logfile = config.get("logfile")
        if logfile:
            file_handler = logging.FileHandler(logfile)
            file_handler.setFormatter(logging.Formatter(config.get("format", "%(asctime)s - %(levelname)s - %(message)s")))
            self.logger.addHandler(file_handler)

        # Log to console if enabled
        if config.get("console", False):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(config.get("format", "%(asctime)s - %(levelname)s - %(message)s")))
            self.logger.addHandler(console_handler)

    def info(self, message):
        """Log an informational message."""
        self.logger.info(message)

    def warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message):
        """Log an error message."""
        self.logger.error(message)

    def debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)

    def critical(self, message):
        """Log a critical message."""
        self.logger.critical(message)
