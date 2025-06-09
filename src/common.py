"""Holds common utility functionality for all source scripts."""

import logging
import os
import signal
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_LEVEL = os.getenv("BOT_LOG_LEVEL", logging.INFO)
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s :: %(message)s"
MANUAL_RUN = signal.getsignal(signal.SIGHUP) == signal.SIG_DFL


# ------------------------------------------------------------------------
# Code inspired from https://stackoverflow.com/a/6290946
# ------------------------------------------------------------------------
class MyFormatter(logging.Formatter):
    """Custom formatter to modify logging timestamp format."""

    converter = datetime.fromtimestamp

    # --------------------------------------------------------------------
    def formatTime(self, record: logging.LogRecord, datefmt: str = None) -> str:
        """
        Handle formatting time for log output.

        :param record:  a log record containing log information
        :param datefmt: optional format string for datetime.strftime
        :returns: a formatted date string
        """
        ct: datetime = self.converter(record.created)

        if datefmt:
            logger_format = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            logger_format = "%s.%03d" % (t, record.msecs)

        return logger_format


# ------------------------------------------------------------------------
def setup_logging(
    name: str = __name__,
    level: int = LOG_LEVEL,
    to_stdout: bool = MANUAL_RUN,
    fresh: bool = False,
) -> logging.Logger:
    """
    Set up logging and return a logging object.

    :param name:      a name to label logs by
    :param level:     a logging level to output, default is INFO
    :param to_stdout: whether to log to stdout or not
    :param fresh:     whether to start log file from scratch
    :returns: a logger object
    """
    # Create logs directory if it doesn't already exist
    log_file_path = Path(os.getcwd(), "logs", name + ".log")
    log_file_path.parent.mkdir(exist_ok=True)
    if fresh and log_file_path.is_file():
        log_file_path.unlink()

    formatter = MyFormatter(fmt=LOG_FORMAT)

    handlers = []

    # Add a rotating log handler
    log_handler = RotatingFileHandler(
        log_file_path,
        mode="a",
        maxBytes=10 * 1024 * 1024,
        backupCount=2,
        encoding=None,
        delay=False,
    )
    log_handler.setFormatter(formatter)
    handlers.append(log_handler)

    # If to_stdout is true, also log to stdout
    if to_stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        handlers.append(stdout_handler)

    # Set up basic logging with the two handlers
    logging.basicConfig(level=level, handlers=handlers)

    return logging.getLogger(name)
