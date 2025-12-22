import logging
import sys


LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | " "%(filename)s:%(lineno)d | %(message)s"
)


def setup_logging(level: str = "INFO") -> None:
    """
    Configure global logging settings.
    Call this ONCE at application startup.
    """
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a module-level logger.
    """
    return logging.getLogger(name)
