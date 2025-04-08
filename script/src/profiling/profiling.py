import re
import pandas as pd
import datetime
import json
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, isnan, regexp_extract


# Load environment variables
load_dotenv(".env", override=True)

class Profiling:
    def __init__(self, data, table_name) -> None:
        """
        Initialize data and table_name for profiling
        
        Parameters:
        data: PySpark DataFrame
        table_name: Name of the table/dataset being profiled
        """
        if not isinstance(data, SparkDataFrame):
            raise TypeError("Input data must be a PySpark DataFrame")
            
        self.data = data
        self.table_name = table_name
        self.list_columns = data.columns

    def get_columns(self):
        """
        Get columns name from data
        """
        return self.list_columns
    
    def selected_columns(self, data_type_list, unique_column_list, missing_column_list, date_column_list, negative_value_column_list):
        """
        Define which columns will be used for profiling 
        a. check data type: data_type_list
        b. check unique value: unique_column_list
        c. check percentage missing value: missing_column_list
        d. check percentage valid date: date_column_list
        """
        self.data_type_list = data_type_list
        self.unique_column_list = unique_column_list
        self.missing_column_list = missing_column_list
        self.date_column_list = date_column_list
        self.negative_value_column_list = negative_value_column_list
        self.column_profiling = list(set(self.data_type_list + self.unique_column_list + 
                                     self.missing_column_list + self.date_column_list + self.negative_value_column_list))

    def check_data_type(self, col_name: str):
        """
        Check data type of column
        """
        dtype = self.data.schema[col_name].dataType
        return str(dtype)
    
    def check_value(self, col_name: str):
        """
        Check unique value of column
        """
        # For PySpark, we use distinct() and collect() 
        # Be careful with this for large datasets
        distinct_values = self.data.select(col_name).distinct().collect()
        list_value = [row[0] for row in distinct_values]
        
        return list_value
    
    def get_percentage_missing_values(self, col_name: str):
        """
        Check percentage missing value of column
        """
        # Count total rows
        total_count = self.data.count()
        
        # Count null or NaN values
        missing_count = self.data.filter(
            col(col_name).isNull() | 
            isnan(col(col_name))
        ).count()
        
        # Calculate percentage
        percentage_null_value = (missing_count / total_count) * 100 if total_count > 0 else 0
        
        return percentage_null_value
    
    def validation_date(self, date_col):
        """
        Create a UDF to check valid date format (dd/mm/yy)
        
        Returns a column expression that can be used in a select statement
        """
        pattern = r'\b(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{2}\b'
        return regexp_extract(col(date_col), pattern, 0) != ""

    
    def get_percentage_valid_date(self, col_name: str):
        """
        Check percentage valid date of column
        """
        # Convert column to string if needed
        temp_data = self.data.withColumn(
            col_name + "_str", 
            col(col_name).cast("string")
        )
        
        # Apply date validation
        temp_data = temp_data.withColumn(
            col_name + "_valid_date",
            regexp_extract(col(col_name + "_str"), 
                          r'\b\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])\b', 0) != ""
        )
        
        # Count total and valid rows
        total_count = temp_data.count()
        valid_count = temp_data.filter(col(col_name + "_valid_date") == True).count()
        
        # Calculate percentage
        percentage = (valid_count / total_count) * 100 if total_count > 0 else 0
        
        return percentage
        



    def check_negative_value(self, col_name: str) -> int:
        """
        Check if column has negative value and return the count of negative values.
        Returns 0 if no negative values are found.
        """
        data_type = self.data.schema[col_name].dataType

        # For numeric column
        if isinstance(data_type, NumericType):
            negative_count = self.data.filter(F.col(col_name) < 0).count()
            return negative_count

        # for string column
        elif isinstance(data_type, StringType):
            # remove $ sign
            cleaned_col = F.regexp_replace(F.col(col_name), r'[\$,]', '').cast("double")
            # filter negative values
            negative_count = self.data.filter(cleaned_col.isNotNull() & (cleaned_col < 0)).count()

            return negative_count

        # if data type is not support
        else:
            raise ValueError(f"Tipe data kolom {col_name} tidak didukung untuk pengecekan nilai negatif.")




    
    def save_report(self):
        """
        Save report to a JSON file in the path specified in .env file
        
        Returns:
        str: Path of the saved file
        """
        # Get path from .env file
        base_path = os.getenv("FILE_PATH", ".")  # Default to current directory if not found
        
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        file_name = f"{self.table_name}_{current_date}.json"
        file_path = os.path.join(base_path, file_name)
        
        # Make sure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.dict_report, f, indent=4)
        
        return f"Report saved as {file_path}"

    def reporting(self):
        """
        Generate profiling report
        """
        self.dict_report = {
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d"),
            "report": {}
        }
        
        for col in self.list_columns:
            self.dict_report["report"][col] = {}
            
            if col in self.data_type_list:
                self.dict_report["report"][col]["data_type"] = self.check_data_type(col)
                
            if col in self.unique_column_list:
                self.dict_report["report"][col]["unique_value"] = self.check_value(col)
                
            if col in self.missing_column_list:
                self.dict_report["report"][col]["percentage_missing_value"] = self.get_percentage_missing_values(col)
                
            if col in self.date_column_list:
                self.dict_report["report"][col]["percentage_valid_date"] = self.get_percentage_valid_date(col)

            if col in self.negative_value_column_list:
                self.dict_report["report"][col]["Total Negative Values"] = self.check_negative_value(col)
        
        print(self.dict_report)
        save_result = self.save_report()
        return self.dict_report