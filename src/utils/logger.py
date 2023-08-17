import logging


def get_logger() -> logging.Logger:
    """
    Create and initialize logger
    Returns:
        logging.Logger: Initialized logger
    """
    logger = logging.getLogger("Hema")
    c_handler = logging.StreamHandler()
    logger.addHandler(c_handler)
    logger.setLevel(logging.DEBUG)
    return logger
