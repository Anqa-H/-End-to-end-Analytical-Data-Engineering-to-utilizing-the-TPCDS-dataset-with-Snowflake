CREATE OR REPLACE PROCEDURE TPCDS.ANALYTICS.populating_customer_dimension_using_scd_type_2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN

-- Merge operation to synchronize data between the target and source tables
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
-- Matching conditions based on various columns between the target and source tables
ON  t1.C_SALUTATION=t2.C_SALUTATION
    AND t1.C_PREFERRED_CUST_FLAG=t2.C_PREFERRED_CUST_FLAG 
    AND coalesce(t1.C_FIRST_SALES_DATE_SK, 0) = coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    AND t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
    AND t1.C_LOGIN=t2.C_LOGIN
    AND coalesce(t1.C_CURRENT_CDEMO_SK,0) = coalesce(t2.C_CURRENT_CDEMO_SK,0)
    AND t1.C_FIRST_NAME=t2.C_FIRST_NAME
    AND coalesce(t1.C_CURRENT_HDEMO_SK,0) = coalesce(t2.C_CURRENT_HDEMO_SK,0)
    AND t1.C_CURRENT_ADDR_SK=t2.C_CURRENT_ADDR_SK
    AND t1.C_LAST_NAME=t2.C_LAST_NAME
    AND t1.C_CUSTOMER_ID=t2.C_CUSTOMER_ID
    AND coalesce(t1.C_LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    AND coalesce(t1.C_BIRTH_MONTH,0) = coalesce(t2.C_BIRTH_MONTH,0)
    AND t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    AND coalesce(t1.C_BIRTH_YEAR,0) = coalesce(t2.C_BIRTH_YEAR,0)
    AND coalesce(t1.C_BIRTH_DAY,0) = coalesce(t2.C_BIRTH_DAY,0)
    AND t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    AND coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) = coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
-- When a match is not found, insert the record from the source table into the target table
WHEN NOT MATCHED 
THEN INSERT (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, 
    C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,    --  this is a column name to store the start date of the record
    END_DATE      --  this is a column name to store the end date of the record
)
-- Values to be inserted into the target table from the source table
VALUES (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, 
    t2.C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),  -- Current date for the start date of the record
    NULL             -- NULL value for the end date of the record, indicating it's the current version
);

-- Retrieve data from the target table after the merge operation
SELECT * FROM TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;

-- Merge operation to update records in the target table when matching conditions are met
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
-- Matching conditions based on various columns between the target and source tables
ON  t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
-- When a match is found and certain column values differ, update the end date of the record in the target table
WHEN MATCHED
    AND (
    t1.C_SALUTATION!=t2.C_SALUTATION
    OR t1.C_PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR coalesce(t1.C_FIRST_SALES_DATE_SK, 0) != coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    OR t1.C_LOGIN!=t2.C_LOGIN
    OR coalesce(t1.C_CURRENT_CDEMO_SK,0) != coalesce(t2.C_CURRENT_CDEMO_SK,0)
    OR t1.C_FIRST_NAME!=t2.C_FIRST_NAME
    OR coalesce(t1.C_CURRENT_HDEMO_SK,0) != coalesce(t2.C_CURRENT_HDEMO_SK,0)
    OR t1.C_CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.C_LAST_NAME!=t2.C_LAST_NAME
    OR t1.C_CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR coalesce(t1.C_LAST_REVIEW_DATE_SK,0) != coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(t1.C_BIRTH_MONTH,0) != coalesce(t2.C_BIRTH_MONTH,0)
    OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR coalesce(t1.C_BIRTH_YEAR,0) != coalesce(t2.C_BIRTH_YEAR,0)
    OR coalesce(t1.C_BIRTH_DAY,0) != coalesce(t2.C_BIRTH_DAY,0)
    OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) != coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
    ) 
-- Update operation to set the end date of the record in the target table to the current date
THEN UPDATE SET
    end_date = current_date();



-- Create or replace a table named CUSTOMER_DIM in the ANALYTICS schema, 
-- derived from a SELECT query joining data from the CUSTOMER_SNAPSHOT table 
-- in the INTERMEDIATE schema with various dimension tables in the RAW_AIR schema.

create or replace table TPCDS.ANALYTICS.CUSTOMER_DIM as
    (select 
        -- Selecting columns from CUSTOMER_SNAPSHOT table
        C_SALUTATION,
        C_PREFERRED_CUST_FLAG,
        C_FIRST_SALES_DATE_SK,
        C_CUSTOMER_SK,
        C_LOGIN,
        C_CURRENT_CDEMO_SK,
        C_FIRST_NAME,
        C_CURRENT_HDEMO_SK,
        C_CURRENT_ADDR_SK,
        C_LAST_NAME,
        C_CUSTOMER_ID,
        C_LAST_REVIEW_DATE_SK,
        C_BIRTH_MONTH,
        C_BIRTH_COUNTRY,
        C_BIRTH_YEAR,
        C_BIRTH_DAY,
        C_EMAIL_ADDRESS,
        C_FIRST_SHIPTO_DATE_SK,
        -- Joining with customer_address table to include additional address information
        CA_STREET_NAME,
        CA_SUITE_NUMBER,
        CA_STATE,
        CA_LOCATION_TYPE,
        CA_COUNTRY,
        CA_ADDRESS_ID,
        CA_COUNTY,
        CA_STREET_NUMBER,
        CA_ZIP,
        CA_CITY,
        CA_GMT_OFFSET,
        -- Joining with customer_demographics table to include demographic information
        CD_DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CD_CREDIT_RATING,
        CD_EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT,
        CD_GENDER,
        -- Joining with household_demographics table to include household demographic information
        HD_BUY_POTENTIAL,
        HD_DEP_COUNT,
        HD_VEHICLE_COUNT,
        HD_INCOME_BAND_SK,
        -- Joining with income_band table to include income band information
        IB_LOWER_BOUND,
        IB_UPPER_BOUND,
        START_DATE,
        END_DATE
    from 
        TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
        -- Performing left joins with dimension tables to enrich customer data
        LEFT JOIN tpcds.raw.customer_address ON c_current_addr_sk = ca_address_sk
        LEFT join tpcds.raw.customer_demographics ON c_current_cdemo_sk = cd_demo_sk
        LEFT join tpcds.raw.household_demographics ON c_current_hdemo_sk = hd_demo_sk
        LEFT join tpcds.raw.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
        where end_date is null -- Filtering the data to include only records where the END_DATE is null, indicating they represent the current version of customer information.
    );  



-- Counting the number of records in the RAW.CUSTOMER table where C_FIRST_SALES_DATE_SK is null,
-- to assess the completeness of the data in terms of first sales date information.
select count(*) from TPCDS.RAW.CUSTOMER where C_FIRST_SALES_DATE_SK is null; --3518 records

  END
  $$;


CREATE OR REPLACE TASK tpcds.ANALYTICS.creating_customer_dimension_using_scd_type_2
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL tpcds.ANALYTICS.populating_customer_dimension_using_scd_type_2();

  ALTER TASK tpcds.ANALYTICS.creating_customer_dimension_using_scd_type_2 RESUME;
  EXECUTE TASK tpcds.ANALYTICS.creating_customer_dimension_using_scd_type_2;
  DROP TASK tpcds.ANALYTICS.creating_customer_dimension_using_scd_type_2;
  DROP tpcds.ANALYTICS.populating_customer_dimension_using_scd_type_2();

  TRUNCATE TABLE TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;
  TRUNCATE TABLE TPCDS.ANALYTICS.CUSTOMER_DIM;
