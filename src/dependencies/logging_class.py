import logging,os
import atexit

class FileSeparatorHandler(logging.FileHandler):
    """Custom FileHandler that writes a separator line after each log record."""

    def emit(self, record):
        """Emit a log record and write a separator line."""
        super().emit(record)

class LoggingConfig:
    """Logging configuration utility for setting up a logger with FileHandler and StreamHandler."""

    def __init__(self, config={}):
        """
        Initialize the LoggingConfig.
        :param log_level: The desired log level for the logger.
        """
        self.file_path=config.get('file_path')
        self.log_level = config.get("log_level")
        self.formatter=config.get('log_formatter')

    def configure_logger(self):
        """
        Configure the logger with FileHandler and StreamHandler.
        :return: The configured logger object.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)

        # Create a formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - Line %(lineno)d - %(message)s')

        # Create a FileHandler with custom FileSeparatorHandler
        file_handler = FileSeparatorHandler(self.file_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # Create a StreamHandler to display logs on the console
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        steam_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(steam_handler)

        # Register atexit handler to write separator line on program exit
        atexit.register(self._write_separator_line)

        return self.logger

    def _write_separator_line(self):
        """
        Write a separator line to the log file.
        This method is automatically called on program exit.
        """
        separator = '-' * 150
        file_handler = next((handler for handler in self.logger.handlers if isinstance(handler, FileSeparatorHandler)),
                            None)
        if file_handler:
            file_handler.stream.write(f"{separator}\n")


if __name__ == "__main__":
    # Create a LoggingConfig instance with log level INFO
    config = LoggingConfig(logging.INFO)

    # Configure the logger
    config.configure_logger()

    # Get the configured logger object
    logger = config.logger

    # Log some entries
    logger.info('Log entry 1')
    logger.info('Log entry 2')


