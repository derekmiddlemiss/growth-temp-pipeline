import logging
from src.config import config


def setup_logger():
    log_level = logging.DEBUG if config("DEBUG", cast=bool) else logging.INFO
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] %(levelname)s [module=%(name)s, line=%(lineno)s] %(message)s",
    )
