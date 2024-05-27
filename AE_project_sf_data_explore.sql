-- Retrieving the calendar date and corresponding date_sk from the date dimension table, sorted by calendar date, limited to 100 records.
select cal_dt, d_date_sk from TPCDS.RAW.DATE_DIM order by 1 limit 100;

-- Retrieving the calendar date corresponding to the minimum CS_SOLD_DATE_SK in the catalog sales table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select min(CS_SOLD_DATE_SK) from TPCDS.RAW.CATALOG_SALES); -- 2021-01-01

-- Retrieving the calendar date corresponding to the maximum CS_SOLD_DATE_SK in the catalog sales table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select max(CS_SOLD_DATE_SK) from TPCDS.RAW.CATALOG_SALES); -- 2023-10-14

-- Retrieving the calendar date corresponding to the minimum WS_SOLD_DATE_SK in the web sales table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select min(WS_SOLD_DATE_SK) from TPCDS.RAW.WEB_SALES); -- 2021-01-02

-- Retrieving the calendar date corresponding to the maximum WS_SOLD_DATE_SK in the web sales table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select max(WS_SOLD_DATE_SK) from TPCDS.RAW.WEB_SALES); -- 2023-10-14

-- Retrieving the calendar date corresponding to the minimum INV_DATE_SK in the inventory table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select min(INV_DATE_SK) from TPCDS.RAW.INVENTORY); -- 2021-01-01

-- Retrieving the calendar date corresponding to the maximum INV_DATE_SK in the inventory table.
select cal_dt from TPCDS.RAW.DATE_DIM where d_date_sk = (select max(INV_DATE_SK) from TPCDS.RAW.INVENTORY); -- 2023-12-03

-- Query to determine the frequency of data received in the catalog sales fact table per day.
-- It provides the calendar date, CS_SOLD_DATE_SK, and the count of records grouped by calendar date and CS_SOLD_DATE_SK.
-- This helps understand how often data is recorded in the catalog sales table.
select 
    d.cal_dt, 
    cs.CS_SOLD_DATE_SK,
    count(*) as count
from
    TPCDS.RAW.CATALOG_SALES cs
INNER JOIN TPCDS.RAW.DATE_DIM d
    ON cs.CS_SOLD_DATE_SK=d.d_date_sk
group by 1,2
order by 1 desc
limit 10;  --About 1000 records everyday

-- Query to determine the frequency of data received in the web sales fact table per day.
-- It provides the calendar date, WS_SOLD_DATE_SK, and the count of records grouped by calendar date and WS_SOLD_DATE_SK.
-- This helps understand how often data is recorded in the web sales table.
select 
    d.cal_dt, 
    ws.WS_SOLD_DATE_SK,
    count(*) as count
from
    TPCDS.RAW.WEB_SALES ws
INNER JOIN TPCDS.RAW.DATE_DIM d
    ON ws.WS_SOLD_DATE_SK=d.d_date_sk
group by 1,2
order by 1 desc
limit 10; --About 500 records everyday

-- Retrieving sample data from the catalog sales fact table to understand its contents.
select * from TPCDS.RAW.CATALOG_SALES limit 100;

-- Verifying the connection between catalog sales and date dimension using cs_sold_date_sk.
-- This query retrieves cs_sold_date_sk from the fact table along with corresponding date dimension attributes to verify the relationship.
select
    fact.cs_sold_date_sk,
    dim.*
from 
    TPCDS.RAW.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW.DATE_DIM as dim
    ON dim.d_date_sk = fact.cs_sold_date_sk
limit 5;

-- Verifying the connection between catalog sales and time dimension using cs_sold_time_sk.
-- This query retrieves cs_sold_time_sk from the fact table along with corresponding time dimension attributes to verify the relationship.
select
    fact.cs_sold_time_sk,
    dim.*
from 
    TPCDS.RAW.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW.TIME_DIM as dim
    ON dim.t_time_sk = fact.cs_sold_time_sk
limit 5;

-- Verifying the connection between catalog sales and customer dimension using cs_bill_customer_sk.
-- This query retrieves cs_bill_customer_sk from the fact table along with corresponding customer dimension attributes to verify the relationship.
select
    fact.cs_bill_customer_sk,
    dim.*
from 
    TPCDS.RAW.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW.CUSTOMER as dim
    ON dim.c_customer_sk = fact.cs_bill_customer_sk
limit 5;

-- Verifying the connection between catalog sales and catalog page dimension using cs_catalog_page_sk.
-- This query retrieves cs_catalog_page_sk from the fact table along with corresponding catalog page dimension attributes to verify the relationship.
select
    fact.cs_catalog_page_sk,
    dim.*
from 
    TPCDS.RAW.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW.CATALOG_PAGE as dim
    ON dim.cp_catalog_page_sk = fact.cs_catalog_page_sk
limit 5;

-- Retrieving sample data from the web sales fact table to understand its contents.
select * from TPCDS.RAW.WEB_SALES limit 100;

-- Verifying the connection between web sales and web page dimension using ws_web_page_sk.
-- This query retrieves ws_web_page_sk from the fact table along with corresponding web page dimension attributes to verify the relationship.
select
    fact.ws_web_page_sk,
    dim.*
from 
    TPCDS.RAW.WEB_SALES as fact
INNER JOIN TPCDS.RAW.WEB_PAGE as dim
    ON dim.wp_web_page_sk = fact.ws_web_page_sk
limit 5;

-- Verifying the connection between web sales and web site dimension using ws_web_site_sk.
-- This query retrieves ws_web_site_sk from the fact table along with corresponding web site dimension attributes to verify the relationship.
select
    fact.ws_web_site_sk,
    dim.*
from 
    TPCDS.RAW.WEB_SALES as fact
INNER JOIN TPCDS.RAW.WEB_SITE as dim
    ON dim.web_site_sk = fact.ws_web_site_sk
limit 5;

-- Query to identify the frequency of inventory records for each item and date.
-- It provides the inventory date, item_sk, and the count of records grouped by inventory date and item_sk.
-- This helps understand how often inventory data is recorded for each item.
select 
    inv_date_sk, 
    inv_item_sk,
    count(*) as count
from
    TPCDS.RAW.INVENTORY
group by 1,2
order by 2,1;

-- Query to determine the frequency of sales for a specific item (inv_item_sk = 1) recorded in the inventory table.
-- It provides the calendar date and week number for each sale of the item, helping understand the frequency of sales recorded in the inventory table.
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
    TPCDS.RAW.DATE_DIM as date
INNER JOIN TPCDS.RAW.INVENTORY as inv
    ON date.d_date_sk=inv.inv_date_sk
    AND inv_item_sk=1
ORDER BY 1,2; -- inventory table records it every week

-- Query to determine the frequency of sales for a specific item (inv_item_sk = 1) recorded in the web sales table.
-- It provides the calendar date and week number for each sale of the item, helping understand the frequency of sales recorded in the web sales table.
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
  TPCDS.RAW.WEB_SALES as ws
INNER JOIN TPCDS.RAW.DATE_DIM as date
    ON date.d_date_sk = ws.ws_sold_date_sk
INNER JOIN TPCDS.RAW.ITEM as item
    ON item.i_item_sk=ws.ws_item_sk
    AND ws_item_sk=1
ORDER BY 1,2; -- Erratic sales with every day as a lowest common factor

-- Query to determine the frequency of sales for a specific item (inv_item_sk = 1) recorded in the catalog sales table.
-- It provides the calendar date and week number for each sale of the item, helping understand the frequency of sales recorded in the catalog sales table.
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
  TPCDS.RAW.CATALOG_SALES as cs
INNER JOIN TPCDS.RAW.DATE_DIM as date
    ON date.d_date_sk = cs.cs_sold_date_sk
INNER JOIN TPCDS.RAW.ITEM as item
    ON item.i_item_sk=cs.cs_item_sk
    AND cs_item_sk=1
ORDER BY 1,2; -- Erratic sales with every day as a lowest common factor

-- Query to retrieve distinct items along with their product names from the item dimension table.
-- This helps understand the variety of items available.
select
    DISTINCT
    i_item_sk,
    i_product_name
from
    TPCDS.RAW.ITEM
ORDER BY 1;

-- Query to count the total number of distinct customers available in the customer dimension table.
-- This helps understand the total count of customers.
select count(distinct c_customer_sk) from TPCDS.RAW.CUSTOMER; --100000 customers

-- Adding descriptive comments to the table and column level to provide information about their purpose.
COMMENT ON TABLE TPCDS.RAW.DATE_DIM IS 'This table is used to convert d_date_sk key in other tables to calendar date. This is the date dimension in the source.';

COMMENT ON COLUMN TPCDS.RAW.DATE_DIM.CAL_DT IS 'Calendar Date';

-- Retrieving calendar date and date_sk from the date dimension table, ordered by calendar date, limited to 100 records.
select cal_dt, d_date_sk from TPCDS.RAW.date_dim order by 1 limit 100;

-- Retrieving all columns from the date dimension table where the calendar date is '2021-01-01'.
select * from TPCDS.RAW.DATE_DIM where cal_dt='2021-01-01';
