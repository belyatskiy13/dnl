import logging
from sys import stdout


class Logger:
    def __new__(self, logger_name: str):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        format = "%(levelname)s - %(name)s:\t%(message)s"
        logFormatter = logging.Formatter(format)
        consoleHandler = logging.StreamHandler(stdout)
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)
        return logger
