from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import csv


load_dotenv(".env")
LOG_POSTGRES_HOST = os.getenv("LOG_POSTGRES_HOST")
LOG_POSTGRES_DB= os.getenv("LOG_POSTGRES_DB")
LOG_POSTGRES_USER= os.getenv("LOG_POSTGRES_USER")
LOG_POSTGRES_PASSWORD= os.getenv("LOG_POSTGRES_PASSWORD")
LOG_POSTGRES_PORT= os.getenv("LOG_POSTGRES_PORT")

SRC_POSTGRES_HOST= os.getenv("SRC_POSTGRES_HOST")
SRC_POSTGRES_DB= os.getenv("SRC_POSTGRES_DB")
SRC_POSTGRES_USER= os.getenv("SRC_POSTGRES_USER")
SRC_POSTGRES_PASSWORD= os.getenv("SRC_POSTGRES_PASSWORD")
SRC_POSTGRES_PORT= os.getenv("SRC_POSTGRES_PORT")

DWH_POSTGRES_HOST= os.getenv("DWH_POSTGRES_HOST")
DWH_POSTGRES_DB= os.getenv("DWH_POSTGRES_DB")
DWH_POSTGRES_USER= os.getenv("DWH_POSTGRES_USER")
DWH_POSTGRES_PASSWORD= os.getenv("DWH_POSTGRES_PASSWORD")
DWH_POSTGRES_PORT= os.getenv("DWH_POSTGRES_PORT")

def log_engine():
    DB_URL = f"jdbc:postgresql://{LOG_POSTGRES_HOST}:{LOG_POSTGRES_PORT}/{LOG_POSTGRES_DB}"
    return DB_URL, LOG_POSTGRES_USER, LOG_POSTGRES_PASSWORD

def src_engine():
    DB_URL = f"jdbc:postgresql://{SRC_POSTGRES_HOST}:{SRC_POSTGRES_PORT}/{SRC_POSTGRES_DB}"
    return DB_URL, SRC_POSTGRES_USER, SRC_POSTGRES_PASSWORD

def dwh_engine():
    DB_URL = f"jdbc:postgresql://{DWH_POSTGRES_HOST}:{DWH_POSTGRES_PORT}/{DWH_POSTGRES_DB}"
    return DB_URL, DWH_POSTGRES_USER, DWH_POSTGRES_PASSWORD


def load_log(spark: SparkSession, log_msg):
    DB_URL, LOG_POSTGRES_USER , LOG_POSTGRES_PASSWORD = log_engine()
    table_name = "etl_log"
 
    # set config
    connection_properties = {
        "user": LOG_POSTGRES_USER,
        "password": LOG_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    log_msg.write.jdbc(url = DB_URL,
                  table = table_name,
                  mode = "append",
                  properties = connection_properties)
    
def log_to_csv(log_msgs: list, filename: str):
    # Check if the file exists
    file_exists = os.path.isfile(filename)

    # Define the column headers
    headers = ["step", "status", "process", "source", "table_name", "etl_date", "error_msg"]

    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write the header only if the file doesn't exist
        if not file_exists:
            writer.writeheader()

        # Append the log messages
        for log_msg in log_msgs:
            writer.writerow(log_msg.asDict())  # Convert Row object to dictionary