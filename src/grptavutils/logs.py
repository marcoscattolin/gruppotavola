import logging


def set_logging(log_level=logging.WARNING):
    log_format = "%(asctime)s [%(levelname)s] : %(message)s"
    logging.basicConfig(format=log_format)
    _logger = logging.getLogger("_empty_")
    _logger.setLevel(log_level)

    return _logger


logger = set_logging(logging.INFO)
