import logging

def get_logger():
    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(console_handler)

    return logger