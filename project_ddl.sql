-- DDL
-- Creating database for Snowflake TPCDS
--CREATE OR REPLACE DATABASE TPCDS;

-- As Customer Snapshot is not ready, we will create it in seperate schema
CREATE OR REPLACE SCHEMA INTERMEDIATE; 

-- Creating Customer Snapshot Table
CREATE OR REPLACE TABLE TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT (
	C_SALUTATION VARCHAR(16777216),  -- Salutation of the customer
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),  -- Flag indicating preferred customer status
	C_FIRST_SALES_DATE_SK NUMBER(38,0),  -- Date key for the first sales date
	C_CUSTOMER_SK NUMBER(38,0),  -- Customer key
	C_LOGIN VARCHAR(16777216),  -- Customer login information
	C_CURRENT_CDEMO_SK NUMBER(38,0),  -- Current customer demographic key
	C_FIRST_NAME VARCHAR(16777216),  -- Customer's first name
	C_CURRENT_HDEMO_SK NUMBER(38,0),  -- Current household demographic key
	C_CURRENT_ADDR_SK NUMBER(38,0),  -- Current address key
	C_LAST_NAME VARCHAR(16777216),  -- Customer's last name
	C_CUSTOMER_ID VARCHAR(16777216),  -- Customer ID
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),  -- Date key for the last review date
	C_BIRTH_MONTH NUMBER(38,0),  -- Customer's birth month
	C_BIRTH_COUNTRY VARCHAR(16777216),  -- Customer's birth country
	C_BIRTH_YEAR NUMBER(38,0),  -- Customer's birth year
	C_BIRTH_DAY NUMBER(38,0),  -- Customer's birth day
	C_EMAIL_ADDRESS VARCHAR(16777216),  -- Customer's email address
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),  -- Date key for the first ship-to date
	START_DATE TIMESTAMP_NTZ(9),  -- Start date of the record
	END_DATE TIMESTAMP_NTZ(9)  -- End date of the record
);

CREATE OR REPLACE SCHEMA ANALYTICS;


-- Final Customer_Dim
create or replace TABLE TPCDS.ANALYTICS.CUSTOMER_DIM (
	C_SALUTATION VARCHAR(16777216),  -- Salutation of the customer
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),  -- Flag indicating preferred customer status
	C_FIRST_SALES_DATE_SK NUMBER(38,0),  -- Date key for the first sales date
	C_CUSTOMER_SK NUMBER(38,0),  -- Customer key
	C_LOGIN VARCHAR(16777216),  -- Customer login information
	C_CURRENT_CDEMO_SK NUMBER(38,0),  -- Current customer demographic key
	C_FIRST_NAME VARCHAR(16777216),  -- Customer's first name
	C_CURRENT_HDEMO_SK NUMBER(38,0),  -- Current household demographic key
	C_CURRENT_ADDR_SK NUMBER(38,0),  -- Current address key
	C_LAST_NAME VARCHAR(16777216),  -- Customer's last name
	C_CUSTOMER_ID VARCHAR(16777216),  -- Customer ID
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),  -- Date key for the last review date
	C_BIRTH_MONTH NUMBER(38,0),  -- Customer's birth month
	C_BIRTH_COUNTRY VARCHAR(16777216),  -- Customer's birth country
	C_BIRTH_YEAR NUMBER(38,0),  -- Customer's birth year
	C_BIRTH_DAY NUMBER(38,0),  -- Customer's birth day
	C_EMAIL_ADDRESS VARCHAR(16777216),  -- Customer's email address
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),  -- Date key for the first ship-to date
	CA_STREET_NAME VARCHAR(16777216),  -- Street name of customer's address
	CA_SUITE_NUMBER VARCHAR(16777216),  -- Suite number of customer's address
	CA_STATE VARCHAR(16777216),  -- State of customer's address
	CA_LOCATION_TYPE VARCHAR(16777216),  -- Location type of customer's address
	CA_COUNTRY VARCHAR(16777216),  -- Country of customer's address
	CA_ADDRESS_ID VARCHAR(16777216),  -- Address ID
	CA_COUNTY VARCHAR(16777216),  -- County of customer's address
	CA_STREET_NUMBER VARCHAR(16777216),  -- Street number of customer's address
	CA_ZIP VARCHAR(16777216),  -- ZIP code of customer's address
	CA_CITY VARCHAR(16777216),  -- City of customer's address
	CA_GMT_OFFSET FLOAT,  -- GMT offset of customer's address
	CD_DEP_EMPLOYED_COUNT NUMBER(38,0),  -- Number of employed dependents
	CD_DEP_COUNT NUMBER(38,0),  -- Total number of dependents
	CD_CREDIT_RATING VARCHAR(16777216),  -- Credit rating of customer
	CD_EDUCATION_STATUS VARCHAR(16777216),  -- Education status of customer
	CD_PURCHASE_ESTIMATE NUMBER(38,0),  -- Purchase estimate of customer
	CD_MARITAL_STATUS VARCHAR(16777216),  -- Marital status of customer
	CD_DEP_COLLEGE_COUNT NUMBER(38,0),  -- Number of dependent college students
	CD_GENDER VARCHAR(16777216),  -- Gender of customer
	HD_BUY_POTENTIAL VARCHAR(16777216),  -- Buy potential of household
	HD_DEP_COUNT NUMBER(38,0),  -- Total number of dependents in household
	HD_VEHICLE_COUNT NUMBER(38,0),  -- Number of vehicles in household
	HD_INCOME_BAND_SK NUMBER(38,0),  -- Income band key for household
	IB_LOWER_BOUND NUMBER(38,0),  -- Lower bound of income band
	IB_UPPER_BOUND NUMBER(38,0),  -- Upper bound of income band
	START_DATE TIMESTAMP_NTZ(9),  -- Start date of the record
	END_DATE TIMESTAMP_NTZ(9)  -- End date of the record
);


create or replace TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY (
    WAREHOUSE_SK NUMBER(38,0),  -- Warehouse key
	ITEM_SK NUMBER(38,0),  -- Item key
	SOLD_WK_SK NUMBER(38,0),  -- Week key for sales
	SOLD_WK_NUM NUMBER(38,0),  -- Week number for sales
	SOLD_YR_NUM NUMBER(38,0),  -- Year number for sales
	SUM_QTY_WK NUMBER(38,0),  -- Total quantity sold in the week
	SUM_AMT_WK FLOAT,  -- Total amount of sales in the week
	SUM_PROFIT_WK FLOAT,  -- Total profit from sales in the week
	AVG_QTY_DY NUMBER(38,6),  -- Average quantity sold per day
	INV_QTY_WK NUMBER(38,0),  -- Quantity of inventory in the week
	WKS_SPLY NUMBER(38,6),  -- Weeks of supply
	LOW_STOCK_FLG_WK BOOLEAN  -- Flag indicating low stock in the week
);

create or replace TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES (
	WAREHOUSE_SK NUMBER(38,0),  -- Warehouse key
	ITEM_SK NUMBER(38,0),  -- Item key
    SOLD_DATE_SK NUMBER(38,0),  -- Date key for sales
    SOLD_WK_NUM NUMBER(38,0),  -- Week number for sales
    SOLD_YR_NUM NUMBER(38,0),  -- Year number for sales
	DAILY_QTY NUMBER(38,0),  -- Quantity sold per day
	DAILY_SALES_AMT FLOAT,  -- Amount of sales per day
	DAILY_NET_PROFIT FLOAT  -- Net profit per day
);
create or replace TABLE TPCDS.ANALYTICS.ITEM_DIM clone TPCDS.RAW.ITEM;
create or replace TABLE TPCDS.ANALYTICS.WAREHOUSE_DIM clone TPCDS.RAW.WAREHOUSE;
create or replace TABLE TPCDS.ANALYTICS.DATE_DIM clone TPCDS.RAW.DATE_DIM;