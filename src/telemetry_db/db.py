import os
import pyodbc
import logging
from src.config import config

logger = logging.getLogger(__name__)


def get_cursor() -> pyodbc.Cursor:
    host = config("TELEMETRY_DB_HOST")
    port = config("TELEMETRY_DB_PORT")
    db_name = config("TELEMETRY_DB_NAME")

    username = os.getenv("TELEMETRY_DB_USERNAME")
    password = os.getenv("TELEMETRY_DB_PASSWORD")

    # sudo apt install unixodbc
    # curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
    # curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    # sudo apt-get update
    # sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
    # TODO @dsm alpine setup at https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
    # TODO @dsm turning off SSL encryption here not ideal - hopefully won't be an issue on Alpine
    cnxn_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={db_name};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no"
    )
    try:
        cnxn = pyodbc.connect(cnxn_string)
        logger.info("Connected to telemetry DB.")
        return cnxn.cursor()
    except pyodbc.Error as e:
        logger.error(f"Could not connect to telemetry DB. Error: {e}")
        raise e
