-- Getting the last date for which sales data is available
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);

-- Removing partial records from the last date to ensure data integrity
DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=$LAST_SOLD_DATE_SK;

-- Compiling all incremental sales records into a temporary table for processing
CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
    -- Compiling incremental sales records from catalog_sales and web_sales tables
    WITH incremental_sales AS (
        SELECT 
            CS_WAREHOUSE_SK AS warehouse_sk,
            CS_ITEM_SK AS item_sk,
            CS_SOLD_DATE_SK AS sold_date_sk,
            CS_QUANTITY AS quantity,
            cs_sales_price * cs_quantity AS sales_amt,
            CS_NET_PROFIT AS net_profit
        FROM tpcds.raw.catalog_sales
        WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
            AND quantity IS NOT NULL
            AND sales_amt IS NOT NULL
        
        UNION ALL

        SELECT 
            WS_WAREHOUSE_SK AS warehouse_sk,
            WS_ITEM_SK AS item_sk,
            WS_SOLD_DATE_SK AS sold_date_sk,
            WS_QUANTITY AS quantity,
            ws_sales_price * ws_quantity AS sales_amt,
            WS_NET_PROFIT AS net_profit
        FROM tpcds.raw.web_sales
        WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
            AND quantity IS NOT NULL
            AND sales_amt IS NOT NULL
    ),

    -- Aggregating sales records to daily level
    aggregating_records_to_daily_sales AS (
        SELECT 
            warehouse_sk,
            item_sk,
            sold_date_sk, 
            SUM(quantity) AS daily_qty,
            SUM(sales_amt) AS daily_sales_amt,
            SUM(net_profit) AS daily_net_profit 
        FROM incremental_sales
        GROUP BY 1, 2, 3
    ),

    -- Adding week number and year number information
    adding_week_number_and_yr_number AS (
        SELECT 
            *,
            date.wk_num AS sold_wk_num,
            date.yr_num AS sold_yr_num
        FROM aggregating_records_to_daily_sales 
        LEFT JOIN tpcds.raw.date_dim date 
            ON sold_date_sk = d_date_sk
    )

    SELECT 
        warehouse_sk,
        item_sk,
        sold_date_sk,
        MAX(sold_wk_num) AS sold_wk_num,
        MAX(sold_yr_num) AS sold_yr_num,
        SUM(daily_qty) AS daily_qty,
        SUM(daily_sales_amt) AS daily_sales_amt,
        SUM(daily_net_profit) AS daily_net_profit 
    FROM adding_week_number_and_yr_number
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
);

-- Displaying the records in the temporary table
SELECT * FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;

-- Inserting new daily aggregated sales records into the intermediate table
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES (
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT
    warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;
