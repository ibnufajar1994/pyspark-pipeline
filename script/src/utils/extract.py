# Function Extarct with log
from src.utils.helper import load_log, src_engine, log_to_csv
from src.utils.sparks import spark_session
from datetime import datetime


def extract_database(table_name):
    # get config
    DB_URL, SRC_POSTGRES_USER, SRC_POSTGRES_PASSWORD = src_engine()

    # get spark session
    spark = spark_session()

    # set config
    connection_properties = {
        "user": SRC_POSTGRES_USER,
        "password": SRC_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    current_timestamp = datetime.now()
    
    try:
        # read data
        df = spark \
                .read \
                .jdbc(url = DB_URL,
                        table = table_name,
                        properties = connection_properties)
    
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("extract_db", "extraction", "success", "source_db", table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("extract_db", "extraction", "failed", "source_db", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)
        log_to_csv(log_msg.collect(), "log.csv")

PATH = "data/"

def extract_csv(file_name):
    spark = spark_session()

    current_timestamp = datetime.now()

    try:

        df = spark.read.csv(PATH + file_name, header=True)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("extract_csv", "extraction", "success", "csv", file_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("extract_csv", "extraction", "failed", "csv", file_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)
        log_to_csv(log_msg.collect(), "log.csv")