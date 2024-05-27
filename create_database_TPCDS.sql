-- Create or replace a database named TPCDS.
CREATE OR REPLACE DATABASE TPCDS;

-- Create or replace a schema named RAW within the TPCDS database.
CREATE OR REPLACE SCHEMA RAW;

-- Create or replace a schema named INTERMEDIATE within the TPCDS database.
CREATE OR REPLACE SCHEMA INTERMEDIATE;

-- Create or replace a schema named ANALYTICS within the TPCDS database.
CREATE OR REPLACE SCHEMA ANALYTICS;

-- Create a table named inventory within the RAW schema.
-- The table includes columns for inventory date, item, quantity on hand, and warehouse.
create or replace table TPCDS.RAW.inventory(
    inv_date_sk int NOT NULL,
    inv_item_sk int NOT NULL,
    inv_quantity_on_hand int,
    inv_warehouse_sk int NOT NULL
);

-- Create or replace a user named Group_6 with a specified password for authentication.
create or replace user ##
password='##';

-- Grant the role 'accountadmin' to the user .
grant role accountadmin to user ##;

-- Set the current warehouse context to COMPUTE_WH.
use warehouse COMPUTE_WH;

-- Set the current schema context to RAW.
use schema raw;
