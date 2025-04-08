from pyspark.sql import DataFrame
from src.utils.helper import load_log, log_to_csv, dwh_engine
from src.utils.sparks import spark_session
from datetime import datetime
import psycopg2

def load_data(df: DataFrame, table_name: str) -> None:
    '''
    This function is used for load transformed data into data warehouse

    Parameters:
    df (pyspark.sql.DataFrame): The transformed data
    table_name: The name of the target table in data warehouse
    '''
    DB_URL, DWH_POSTGRES_USER, DWH_POSTGRES_PASSWORD = dwh_engine()
    spark = spark_session()

    connection_properties = {
        "user": DWH_POSTGRES_USER,
        "password": DWH_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    current_timestamp = datetime.now()

    #============================== Start Truncate Table Before Load ============================== #
    try:
        # Parse DB_URL for direct PostgreSQL connection
        # Format expected: jdbc:postgresql://host:port/dbname
        db_parts = DB_URL.replace("jdbc:postgresql://", "").split("/")
        host_port = db_parts[0].split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 5432
        dbname = db_parts[1]
        
        # Connect directly to PostgreSQL for truncate operation
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=DWH_POSTGRES_USER,
            password=DWH_POSTGRES_PASSWORD
        )
        
        # Create cursor and execute truncate
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            conn.commit()
            
        conn.close()
        
    except Exception as e:
        print(f"Error during truncate operation: {e}")
        # Log truncate error but continue with the process
        truncate_error_log = spark.sparkContext\
            .parallelize([("Truncate_Table", "Load", "failed", "dwh", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
        load_log(spark, truncate_error_log)
    #============================== End Truncate Table Before Load ============================== #



    try:
         
        # insert dataframe to database table
        df.write.jdbc(url=DB_URL, table=table_name, mode="append", properties=connection_properties)

        log_msg = spark.sparkContext\
            .parallelize([("Load_DB", "Load", "success", "source_db", table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("Load_db", "Load", "failed", "source_db", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])


    finally:
        # load log
        load_log(spark, log_msg)
        log_to_csv(log_msg.collect(), "log.csv")