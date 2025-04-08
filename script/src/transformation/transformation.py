from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, when, lit, regexp_extract, concat, substring, regexp_replace, floor
from datetime import datetime
from src.utils.helper import log_to_csv, load_log
from src.utils.sparks import spark_session
current_timestamp = datetime.now()

spark = spark_session()

class Transformation:
    def __init__(self, data: DataFrame, table_name: str) -> None:
        self.data = data
        self.table_name = table_name
        self.list_columns = data.columns
        
    def transform_customers(self) -> DataFrame:

        try:
                
            #============================== Start Renaming Column ============================== #
            # Select only the specified columns
            self.data = self.data.select("CustomerID", "CustomerDOB", "CustGender", "CustLocation", "CustAccountBalance")
            # Rename the columns
            rename_mappings = {
                "customers": {
                    "CustomerID": "customer_id",
                    "CustomerDOB": "birth_date",
                    "CustGender": "gender",
                    "CustLocation": "location",
                    "CustAccountBalance": "account_balance"
                }
            }
            if self.table_name in rename_mappings:
                for old_col, new_col in rename_mappings[self.table_name].items():
                    self.data = self.data.withColumnRenamed(old_col, new_col)
            
            #============================== End Renaming Column =================================== #
            
            #============================== Start Transform Date Format============================= #
            self.data = self.data.withColumn(
                "birth_date",
                when(
                    regexp_extract(col("birth_date"), "(\\d{1,2})/(\\d{1,2})/(\\d{2})", 3).cast("int") > 25, 
                    to_date(
                        concat(
                            regexp_extract(col("birth_date"), "(\\d{1,2})/(\\d{1,2})/", 1), lit("/"),
                            regexp_extract(col("birth_date"), "\\d{1,2}/(\\d{1,2})/", 1), lit("/19"),
                            regexp_extract(col("birth_date"), "\\d{1,2}/\\d{1,2}/(\\d{2})", 1)
                        ),
                        "d/M/yyyy"
                    )
                ).otherwise(to_date(col("birth_date"), "d/M/yy"))

            )
            #============================== End Transform Date Format============================= #
            
            #============================== Start Clean Gender Column ============================= #
            self.data = self.data.withColumn(
                "gender",
                when(col("gender") == "M", lit("Male"))
                .when(col("gender") == "F", lit("Female"))
                .otherwise(lit("Other"))  # mengubah 'Others' menjadi 'Other' sesuai constraint
            )
            #============================== End Clean Gender Column =============================== #
            
            #============================== Start Cast account_balance data type to float  ============================= #
            self.data = self.data.withColumn(
                "account_balance",
                col("account_balance").cast("float")
            )
            #============================== End Cast account_balance data type to float  =============================== #
            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "success", "csv", "customers", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

            return self.data
        
        except Exception as e:
            print(e)

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "failed", "csv", "customers", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        finally:
        # load log
            load_log(spark, log_msg)
            log_to_csv(log_msg.collect(), "log.csv")

#=================================================END OF CUSTOMERS TRANSFORMATION=================================================#

    def transform_transactions(self) -> DataFrame: # Function to transform transaction table
        # Select only required columns
        self.data = self.data.select('TransactionID','CustomerID', 'TransactionDate', 'TransactionTime', 'TransactionAmount (INR)')
        
        #============================== Start Renaming Column ============================== #
        try:
           # Rename the columns
            rename_mappings = {
                "transactions": {
                    "TransactionID": "transaction_id",
                    "CustomerID": "customer_id",
                    "TransactionDate": "transaction_date",
                    "TransactionTime": "transaction_time",
                    "TransactionAmount (INR)": "transaction_amount"
                }
            }
            if self.table_name in rename_mappings:
                for old_col, new_col in rename_mappings[self.table_name].items():
                    self.data = self.data.withColumnRenamed(old_col, new_col)
            
            #============================== End Renaming Column =================================== #

            #============================== Start Transform Date Format============================= #
            self.data = self.data.withColumn(
                "transaction_date",
                when(
                    regexp_extract(col("transaction_date"), "(\\d{1,2})/(\\d{1,2})/(\\d{2})", 3).cast("int") > 25, 
                    to_date(
                        concat(
                            regexp_extract(col("transaction_date"), "(\\d{1,2})/(\\d{1,2})/", 1), lit("/"),
                            regexp_extract(col("transaction_date"), "\\d{1,2}/(\\d{1,2})/", 1), lit("/19"),
                            regexp_extract(col("transaction_date"), "\\d{1,2}/\\d{1,2}/(\\d{2})", 1)
                        ),
                        "d/M/yyyy"
                    )
                ).otherwise(to_date(col("transaction_date"), "d/M/yy"))

            )
            #============================== End Transform Date Format============================= #

            #============================== Start Transform Transaction Time Format============================= #
            self.data = self.data.withColumn(
                "transaction_time",
                concat(
                    substring(col("transaction_time"), 1, 2), lit(":"),
                    substring(col("transaction_time"), 3, 2), lit(":"),
                    substring(col("transaction_time"), 5, 2)
                )
            )
            #============================== End Transform Transaction Time Format============================= #

            #============================== Start Cast account_balance data type to float  ============================= #
            self.data = self.data.withColumn(
                "transaction_amount",
                col("transaction_amount").cast("float")
            )
            #============================== End Cast account_balance data type to float  =============================== #
            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "success", "csv", "transactions", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

            return self.data
        
        except Exception as e:
            print(e)

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "failed", "csv", "transactions", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        finally:
        # load log
            load_log(spark, log_msg)
            log_to_csv(log_msg.collect(), "log.csv")
#=================================================END OF TRANSACTIONS TRANSFORMATION=================================================#

    def transform_education_status(self) -> DataFrame:
        try: 
            self.data = self.data.select("education_id", "value")

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "success", "database", "education_status", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

            return self.data
        
        except Exception as e:
            print(e)

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "failed", "database", "education_status", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        finally:
        # load log
            load_log(spark, log_msg)
            log_to_csv(log_msg.collect(), "log.csv")
#=================================================END OF EDUCATION_STATUS TRANSFORMATION=================================================#

    def transform_marital_status(self) -> DataFrame:
        
        try:
            self.data = self.data.select("marital_id", "value")

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "success", "database", "marital_status", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

            return self.data
   

        except Exception as e:
            print(e)

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "failed", "database", "marital_status"  , current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        finally:
        # load log
            load_log(spark, log_msg)
            log_to_csv(log_msg.collect(), "log.csv")
#=================================================END OF MARITAL_STATUS TRANSFORMATION=================================================#

    def transform_marketing_campaign_deposit(self) -> DataFrame:
        self.data = self.data.select('loan_data_id','age', 'job', 'marital_id','education_id','default','balance','housing','loan','contact',
                                        'day','month','duration','campaign','pdays','previous','poutcome', 'subscribed_deposit')
        
        table_name = "marketing_campaign_deposit"

            #============================== Start Renaming Column ============================== #
        try:
            # Rename the columns
            rename_mappings = {
                "marketing_campaign_deposit": {
                    "pdays": "days_since_last_campaign",
                    "previous": "previous_campaign_contacts",
                    "poutcome": "previous_campaign_outcome",
                    "TransactionTime": "transaction_time",
                    "TransactionAmount (INR)": "transaction_amount"
                }
            }
            if self.table_name in rename_mappings:
                for old_col, new_col in rename_mappings[self.table_name].items():
                    self.data = self.data.withColumnRenamed(old_col, new_col)
            

            # Remove  $ sign on "balance" then convert the value into INT
            self.data = self.data.withColumn("balance", regexp_replace(col("balance"), "\\$", "").cast("int"))

            # Create New Column "duration_in_year"
            self.data = self.data.withColumn("duration_in_year", floor(col("duration") / 365).cast("int"))

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "success", "database", "marketing_campaign_deposit", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])         

            return self.data
        

        except Exception as e:
            print(e)

            log_msg = spark.sparkContext\
            .parallelize([("Transform_Data", "Transform", "failed", "database", "marketing_campaign_deposit", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        finally:
        # load log
            load_log(spark, log_msg)
            log_to_csv(log_msg.collect(), "log.csv")

#=================================================END OF MARKETING_CAMPAIGN_DEPOSIT TRANSFORMATION=================================================#