
# End to end Analytical Data Engineering to utilizing the TPCDS dataset with Snowflake


## Project Overview
This project involves typical Analytical Data Engineering tasks, including data ingestion from various sources, loading it into the Snowflake data warehouse, and transforming it for Business Intelligence (BI) purposes. The BI tool Metabase is used to generate dashboards and reports from the data warehouse.
![ae_sda_full](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/e58ed1c7-4594-4d3a-80d9-f3c37f01ede2)
## Data Details
### Data Background
The dataset comes from TPCDS, designed for database testing with a focus on Retail Sales. It includes sales records from websites and catalogs, inventory levels for each item in all warehouses, and 15 dimensional tables with information about customers, warehouses, items, and more.
![tbs](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/9c95ae43-f7ff-419f-8a4a-db011faa62e2)

### Dataset Split
#### AWS RDS: All tables except the inventory tables are stored in the Postgres DB in AWS RDS. These tables are refreshed daily with the latest sales data, requiring daily ETL processes.
#### S3 Bucket: The inventory table is stored in an S3 bucket, with a new file containing the latest data deposited daily. The inventory table typically records data at the end of each week.
### Tables in the Dataset
Multiple tables related to customers are consolidated into a unified customer dimension 
![cs](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/d1e9bf66-fc80-4019-8886-a5dc58bbed53)
## Business Requirements
#### Metabase Requirements
Generate dashboards and reports to:

Determine top and bottom-performing items by sales amounts and quantities.
Show items with low inventory levels weekly.
Identify items with low stock levels, including their associated week and warehouse numbers.
####  Snowflake Data Warehouse Requirements
Create new tables in the data warehouse to meet BI requirements:

Consolidate certain raw tables, such as merging various customer-related tables into one.
Establish a new weekly fact table with additional metrics:
- sum_qty_wk: Sum of sales_quantity for the week.
- sum_amt_wk: Sum of sales_amount for the week.
- sum_profit_wk: Sum of net_profit for the week.
- avg_qty_dy: Average daily sales_quantity for the week.
- inv_on_hand_qty_wk: Inventory on hand at the end of the week.
- wks_sply: Weeks of supply (inv_on_hand_qty_wk/sum_qty_wk).
- low_stock_flg_wk: Low stock weekly flag.
## Project Infrastructure
The entire infrastructure is constructed in the cloud:

#### Servers: Create several servers on the AWS cloud.
#### Tools: Install tools on these servers, including Airbyte for data ingestion and Metabase for BI.
#### Cloud Data Warehouse: Use Snowflake for data storage and transformation.
#### AWS Lambda: Ingest data from AWS storage (S3).
## Project Phases
### Data Ingestion
#### Airbyte Installation
- Launch EC2 Instances: Deployed two Ubuntu EC2 instances on AWS—one for Metabase and one for Airbyte.
- Install Docker: Installed Docker on both instances.
- Install Airbyte: Set up Airbyte on the designated instance for data ingestion.
![connector](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/3ec40d62-21bb-4a43-a68b-8f57d14dcc2c)
![Screenshot 2024-04-18 111736](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/af66f0c0-c068-4998-9ecb-3a74bd4a3d1b)
#### AWS Lambda Setup
- Lambda Function:Created a Lambda function to download 'inventory.csv' from S3 and upload it to Snowflake.
- Scheduling: Configured the function to run daily at 2 am (Riyadh time).
![Screenshot 2024-04-17 201234](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/e0148ee3-510e-4044-b731-9035bf1df7ad)
![Screenshot 2024-04-18 120626](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/6222fed4-f5c9-40ee-b992-66f1bdb74a15)
### Data Transformation
- Data Transformation: Transformed data within Snowflake from its original structure to the desired format.
- Data Model & ETL Scripts: Developed a data model and ETL scripts to facilitate data loading.
- Schedule Data Loading: Established a regular data loading schedule.
### Data Analysis
- Connect Snowflake to Metabase: Linked Snowflake to Metabase for BI purposes.
- Dashboards & Reports: Created and displayed interactive dashboards and reports in Metabase.
![Screenshot 2024-04-18 065734](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/1f7ec54e-cdf2-4ce3-829b-9a62a92c0638)
![1](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/40e7254e-5491-4fd8-a920-b29580c4a8ca)
![2](https://github.com/Anqa-H/-End-to-end-Analytical-Data-Engineering-to-utilizing-the-TPCDS-dataset-with-Snowflake/assets/80011409/f9703ceb-7899-450c-b28d-44d4ee827191)
