from src.utils.sparks import spark_session
from src.utils.extract import extract_database, extract_csv
from src.profiling.profiling import Profiling
from src.transformation.transformation import Transformation
from src.utils.load_data import load_data
from src.utils.helper import load_log, log_to_csv

if __name__ == "__main__":

    print("====================Start Extracting Data======================")
    # Extract data
    df_csv = extract_csv("new_bank_transaction.csv")
    df_marital_status = extract_database("marital_status")
    df_education_status = extract_database("education_status")
    df_marketing_campaign_deposit = extract_database("marketing_campaign_deposit")

    print("====================Finish Extracting Data======================")

    print("====================Start Profiling Data======================")

    #Create Profiling Instance
    df_csv_profiling = Profiling(data=df_csv, table_name="new_bank_transaction")
    df_marital_status_profiling = Profiling(data=df_marital_status, table_name="marital_status")
    df_education_status_profiling = Profiling(data=df_education_status, table_name="education_status")
    df_marketing_campaign_deposit_profiling = Profiling(data=df_marketing_campaign_deposit, table_name="marketing_campaign_deposit")

    #Get column for all tables
    csv_table = df_csv_profiling.get_columns()
    marital_status_table = df_marital_status_profiling.get_columns()
    education_status_table = df_education_status_profiling.get_columns()
    marketing_campaign_deposit_table = df_marketing_campaign_deposit_profiling.get_columns()

    #Profiling csv table
    unique_column = ['CustGender']
    missing_value_column = csv_table
    date_column = ['CustomerDOB', 'TransactionDate']
    negative_value_column = ['CustAccountBalance','TransactionAmount (INR)']

    df_csv_profiling.selected_columns(csv_table, unique_column, missing_value_column, date_column, negative_value_column)
    report_csv = df_csv_profiling.reporting()

    #Profiling marital_status table
    unique_column = ['value']
    missing_value_column = []
    date_column = []
    negative_value_column = []

    df_marital_status_profiling.selected_columns(marital_status_table, unique_column, missing_value_column, date_column, negative_value_column)
    report_marital_status = df_marital_status_profiling.reporting()

    #Profiling education_status table
    unique_column = ['value']
    missing_value_column = []
    date_column = []
    negative_value_column = []

    df_education_status_profiling.selected_columns(education_status_table, unique_column, missing_value_column, date_column, negative_value_column)
    report_marital_status = df_education_status_profiling.reporting()

    #Profiling marketing_campaign_deposit table
    unique_column = ['job', 'contact']
    missing_value_column = ['loan_data_id', 'age', 'job', 'marital_id', 'education_id', 'balance', 'contact', 'day', 'month', 'duration', 'campaign', 'pdays', 'previous', 'poutcome']
    date_column = []
    negative_value_column = ['balance', 'age']

    df_marketing_campaign_deposit_profiling.selected_columns(marketing_campaign_deposit_table, unique_column, missing_value_column, date_column, negative_value_column)
    report_marketing_campaign = df_marketing_campaign_deposit_profiling.reporting()

    print("====================Finish Profiling Data======================")
    print("================================================================")
    print("====================Start Transforming Data======================")
    # Transform data
    df_customers_trans = Transformation(data=df_csv, table_name="customers")
    df_transactions_trans = Transformation(data=df_csv, table_name="transactions")
    
    df_marital_status_trans = Transformation(data=df_marital_status, table_name="marital_status")
    df_education_status_trans = Transformation(data=df_education_status, table_name="education_status")
    df_marketing_campaign_deposit_trans = Transformation(data=df_marketing_campaign_deposit, table_name="marketing_campaign_deposit")
    
    customers_data = df_customers_trans.transform_customers()
    transactions_data = df_transactions_trans.transform_transactions()
    marital_status_data = df_marital_status_trans.transform_marital_status()
    education_status_data = df_education_status_trans.transform_education_status()
    marketing_campaign_deposit_data = df_marketing_campaign_deposit_trans.transform_marketing_campaign_deposit()
    print("====================Finish Transforming Data======================")
    print("================================================================")
    print("====================Start Loading Data======================")
    # Load data
    load_data(df=customers_data, table_name="customers")
    load_data(df=transactions_data, table_name="transactions")
    load_data(df=marital_status_data, table_name="marital_status")
    load_data(df=education_status_data, table_name="education_status")  # Menambahkan baris ini
    load_data(df=marketing_campaign_deposit_data, table_name="marketing_campaign_deposit")
    print("====================Finish Loading Data======================")
    print("================================================================")
    print("====================SUCCESS TO RUN PIPELINE!!!======================")

