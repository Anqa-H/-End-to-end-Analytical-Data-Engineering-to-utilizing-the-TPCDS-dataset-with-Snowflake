-- Checking if there are any NULL values for the customer ID (C_CUSTOMER_SK) in the customer dimension table.
-- If the count is 0, it means there are no NULL values, which confirms that C_CUSTOMER_SK is not null as expected.
select count(*) = 0 from TPCDS.ANALYTICS.customer_dim
where c_customer_sk is null;

-- Testing uniqueness of combinations of warehouse_sk, item_sk, and sold_wk_sk in the weekly sales inventory table.
-- If the count is 0, it means there are no duplicate combinations, indicating that these columns form a unique key.
SELECT count(*) = 0 FROM
(
    SELECT
        warehouse_sk, item_sk, sold_wk_sk
    FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
    GROUP BY 1, 2, 3
    HAVING count(*) > 1
);

-- Performing a relationship test to ensure that each record in the weekly sales inventory table 
-- corresponds to a valid item in the item dimension table.
-- If the count is 0, it means there are no records in the weekly sales inventory table 
-- that do not have a matching item in the item dimension table, confirming the relationship.
SELECT count(*) FROM
(
    SELECT
        dim.i_item_sk 
    FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY fact
    LEFT JOIN TPCDS.ANALYTICS.ITEM_DIM dim
    ON dim.i_item_sk = fact.dim_I_ITEM_SK
    WHERE fact.dim_I_ITEM_SK IS NULL
);

-- Testing if all warehouse_sk values in the weekly sales inventory table are one of the accepted values (1,2,3,4,5,6).
-- If the count is 0, it means there are no warehouse_sk values outside the accepted range, confirming the accepted values.
select count(*) = 0 from TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
where warehouse_sk NOT IN (1,2,3,4,5,6);
