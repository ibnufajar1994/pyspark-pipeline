
![header](https://github.com/user-attachments/assets/59751560-25e3-4dc0-9fbe-8a1f7e8aabab)

# pyspark-pipeline
Build Data Pipeline Using Pyspark

## Content
[1. Overview](#1-overview)  
[2. Source of Dataset](#2-source-of-dataset)   
[3. Problem Overview](#4-requirements-gathering)  
[4. Solution Approach](#5-source-to-target-mapping)  
[5. Validation Rule](#6-validation-rule)  
[6. How to Use This Project](#7-how-to-use-this-project)
   - [Preparations](#71-preparations)
   - [Running the Pipeline](#72-running-the-pipeline)

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



