
![header](https://github.com/user-attachments/assets/59751560-25e3-4dc0-9fbe-8a1f7e8aabab)

# pyspark-pipeline
Build Data Pipeline Using Pyspark

## Content
[1. Overview](#1-overview)  
[2. Source of Dataset](#2-source-of-dataset)  
[3. Problem Overview](#3-problem-overview)  
[4. Solution Approach](#4-solution-approach)  
[5. Data Transformation Rule](#5-data-transformation-rule)  
[6. Data Pipeline Design](#6-data-pipeline-design)  
[7. How to Use This Repository](#7-how-to-use-this-repository)  
   - [Preparations](#preparations)  
   - [Running the Project](#running-the-project)  
   - [Access the Jupyter Notebook](#access-the-jupyter-notebook)
     

# 1. Overview
This repository showcases a project that demonstrates a data pipeline using the ETL method with PySpark to process various data sources. It includes logging features to capture all events within the pipeline. Additionally, it provides a detailed overview of the data pipeline, including data sources, storage destinations, requirement analysis, proposed solutions, and the final pipeline design.

# 2. Source of Dataset
The data set used in this project comes from this [repository](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_exercise_3). The data relates to phone-based direct marketing campaigns carried out by a bank to promote a term deposit product. Several calls were typically required to ascertain whether a client would subscribe ('yes' or 'no') to the product. The dataset including large csv file called "new_bank_transaction.csv". This dataset includes transactional and demographic information for more than 800,000 banking customers, encompassing account balances and transaction specifics.


# 3. Problem Overview
- **Data Sourced from Multiple Origins:** Information is collected from a structured database as well as CSV files, requiring careful integration to maintain consistency.
- **Managing High Data Volume:** Working with large datasets and files can be resource-intensive, potentially impacting system performance.
- **Data Format and Readiness:** The initial raw data may not be in an analysis-ready format, making transformation and optimization necessary.

# 4. Solution Approach

## ETL Overview

This project follows a structured ETL (Extract, Transform, Load) pipeline to process and integrate data from multiple sources into a centralized data warehouse.

---

### 1. Data Profiling

Before initiating the ETL workflow, data profiling is performed to evaluate data quality and structure:
These profiling insights guide necessary preprocessing steps, including data cleaning, normalization, and structural adjustments.

---

### 2. ETL Workflow

A systematic ETL process ensures reliable and scalable data transformation and loading:

#### 🔍 Extract

- Retrieve structured data from the source database.
- Load customer and transaction records from CSV files.

#### 🔄 Transform

- Standardize and format raw fields to match the target schema of the data warehouse.

#### 📦 Load

- Insert the transformed data into optimized warehouse tables.
- Maintain entity relationships to ensure data consistency and integrity.

---

This ETL framework enables efficient data integration, ensuring high-quality, analysis-ready datasets.

# 5. Data Transformation Rule

The transformation phase is handled using a series of PySpark scripts, each focusing on specific data preprocessing tasks to prepare the datasets for loading into the data warehouse.

---

### 1. Data Type Conversion

Ensures columns are cast to their correct data types.

**Examples:**
- Converts `balance` from a string with currency symbols to an integer.
- Parses `transaction_amount` and `account_balance` as doubles.
- Derives `duration_in_year` by converting `duration` from days to years (rounded).

---

### 2. Data Cleaning

Standardizes and cleans values across datasets.

**Examples:**
- In the Customers table, normalizes `customer_gender` values:
  - `M` → `Male`
  - `F` → `Female`
  - Others → `Other`
- Trims whitespace and applies lowercase formatting for consistency.

---
### 3. Date and Time Formatting

Applies uniform formatting to date and time fields.

**Examples:**
- Converts `transaction_date` from `d/M/yy` to `YYYY/MM/DD`.
- Formats `transaction_time` from `HHMMSS` to `HH:MM:SS`.
- Adjusts `CustomerDOB` to include century:
  - Year > 25 → prefixed with `19`
  - Year ≤ 25 → prefixed with `20`

---

### 4. Column Renaming

Aligns column names to a consistent naming convention across tables.

**Examples:**

**Customers Table:**
- `CustomerID` → `customer_id`
- `CustomerDOB` → `birth_date`
- `CustGender` → `gender`

**Transactions Table:**
- `TransactionID` → `transaction_id`
- `TransactionDate` → `transaction_date`

**Marketing Table:**
- `pdays` → `days_since_last_campaign`
- `previous` → `previous_campaign_contacts`

---

### 5. Column Selection

Filters the dataset to retain only relevant fields for analysis.

**Examples:**

**Customers Table:**
- `customer_id`, `birth_date`, `gender`, `location`, `account_balance`

**Transactions Table:**
- `transaction_id`, `customer_id`, `transaction_date`, `transaction_time`, `transaction_amount`

# 6. Data Pipeline Design
**Data Profiling**

![Tanpa judul (681 x 244 piksel)](https://github.com/user-attachments/assets/81c1e66e-375e-4032-99e7-22c0bb9f4f00)

**ETL Pipeline**

![Tanpa judul (681 x 244 piksel) (1)](https://github.com/user-attachments/assets/d6a17cee-14b9-4999-a4c4-72c3cca83acc)


# 7. How to Use This Repository
## Preparations
**Clone the repository:**
   ```bash
   git clone https://github.com/ibnufajar1994/data-pipeline.git
   ```
## Running the project  
   ```bash
   docker compose build --no-cache
   docker compose up -d
   ```
## Access the Jupyter Notebook
look up the logs for pyspark container
   ```bash
  docker logs pyspark_container
   ```
get the token of the jupyternotebook, for example:
   ```bash
http://127.0.0.1:8888/lab?token=3fa3b1cf2c67643874054971f23ee59bdee283b373794847
   ```
copy and paste the token into your browser

