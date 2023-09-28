from logger import setup_logger
from telemetry_db.db import get_cursor

if __name__ == "__main__":
    setup_logger()
    get_cursor()
