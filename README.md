# Sales and Customer Data Warehouse - Medallion Architecture 🚀
## Overview 📑
This project builds a modern data warehouse using SQL Server and follows the medallion architecture. Data from CRM and ERP CSVs are ingested into the Bronze layer, cleaned and transformed in the Silver layer, and structured in Gold layer views for analytics and reporting.
It involves 
- Data Architecture: Creating a Data warehouse using Medallion Architecture
- ETL Pipelines: Extracting, transforming, and loading data from the source systems into the warehouse using stored procedures
- Data Modeling: Developing fact and dimensional tables using VIEWS and modeling them in a star schema
  
![DATA Lineage](docs/Data_Lineage.jpg)

## Data Sources
The following figure explains the data from the two systems and how they integrate

![DATA Integration](docs/Data_Integration.png)

## ETL Pipeline Overview ⚙️
### Bronze Layer - Raw Ingestion
In the bronze layer
- Loaded full CRM and ERP CSV datasets directly into tables.
- No transformations applied at this stage to preserve data fidelity.

### Silver Layer – Data Cleaning & Standardization
Applied core ETL transformations while keeping the original table structure intact:
- Deduplication using ROW_NUMBER() to retain the most recent record per entity.
- String cleaning- trimming whitespace and normalizing case.
- Category standardization (e.g., converting single-letter gender/marital codes to readable values).
- Handling missing and null values using default values or conditional logic.
- Data quality assurance by filtering invalid IDs and inconsistent records.
  
<img src="docs/ETL_Glimpse.jpg" alt="ETL" width="650">


These transformations were encapsulated in a Silver layer stored procedure for consistent processing.

### Gold Layer – Data Modeling
- Built analytical models by joining Silver tables to create fact and dimension tables.
- Implemented using SQL views to keep the Gold layer fully virtual and easily refreshable.
- Ensured a star-schema–like structure optimized for reporting and downstream analytics.
  
![DATA FLOW](docs/Data_Flow.jpg)




## Project Structure


## Future Improvements
