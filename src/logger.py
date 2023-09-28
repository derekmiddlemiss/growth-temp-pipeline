import logging
from src.config import config

log_level = logging.DEBUG if config("DEBUG", cast=bool) else logging.INFO


def setup_logger():
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] %(levelname)s [module=%(name)s, line=%(lineno)s] %(message)s",
    )
