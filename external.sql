--CREATING STORAGE INTEGRATION
create or replace storage integration snowlake_integration
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::745882220951:role/integration-role'
    enabled = true
    storage_allowed_locations = ('s3://snowlake-bucket/salesdata/store_returns/');

    
--DESCRIBING STORAGE INTEGRATION 
DESC INTEGRATION snowlake_integration;

--CREATING EXTERNAL STAGE
create or replace stage external_table_stage
url='s3://snowlake-bucket/salesdata/store_returns/'
storage_integration=snowlake_integration;


list @external_table_stage;

--CREATING FILE FORMAT
create or replace file format snowlake_format type='parquet';

--create external table
create or replace external table snowlake_ext_table 
with location = @external_table_stage file_format = snowlake_format;

select * from snowlake_ext_table limit 10;

 

SELECT $1: SR_RETURNED_DATE_SK:: decimal as SR_RETURNED_DATE_SK FROM snowlake_ext_table limit 10 ;

select COUNT(*) AS total_records FROM snowlake_ext_table;

 
 

select count(*)AS total_records from iceberg_managed_store_returns;
SELECT COUNT(*) AS total_records FROM iceberg_store_returns;
SELECT COUNT(*) AS total_records FROM snowlake_ext_table;
 
MANAGED ICEBERG -- run time :27 ms
UNMANAGED ICEBERG -- run time:59 ms
EXTERNAL TABLES -- run time: 6.1s

 SELECT
    COUNT(*) AS total_records,
    SUM(SR_RETURN_QUANTITY) AS total_return_quantity,
    MIN(SR_RETURN_AMT) AS min_return_amount,
    MAX(SR_RETURN_AMT) AS max_return_amount
FROM
     iceberg_store_returns

     
SELECT
    $1:SR_RETURNED_DATE_SK,
    $1:SR_RETURN_TIME_SK,
    $1:SR_ITEM_SK,
    $1:SR_NET_LOSS
FROM
    snowlake_ext_table
WHERE
    $1:SR_NET_LOSS > 1000
ORDER BY
    $1:SR_RETURN_QUANTITY;

set used_cache_result = false;

--external table for customer data

--CREATING STORAGE INTEGRATION
create or replace storage integration snowlake_integration_ext
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::745882220951:role/snowlake_ext'
    enabled = true
    storage_allowed_locations = ('s3://snowlake-bucket/salesdata/customer/');

--DESCRIBING STORAGE INTEGRATION 
DESC INTEGRATION snowlake_integration_ext;

--CREATING EXTERNAL STAGE
create or replace stage customer_stage
url='s3://snowlake-bucket/salesdata/customer/'
storage_integration=snowlake_integration_ext;


list @customer_stage;

--CREATING FILE FORMAT
create or replace file format snowlake_format type='parquet';

--create external table
create or replace external table customer_ext_table 
with location = @customer_stage file_format = snowlake_format;

set used_cache_result = false;

SELECT
    $1:C_FIRST_NAME,
    $1:C_LAST_NAME,
    $1:C_BIRTH_MONTH,
    $1:C_BIRTH_YEAR
FROM
     customer_ext_table
WHERE
    $1:C_BIRTH_MONTH = 4
    AND $1:C_BIRTH_YEAR = 1985;

SELECT
    $1:C_FIRST_NAME,
    $1:C_LAST_NAME,
    $1:C_EMAIL_ADDRESS
FROM
    customer_ext_table
WHERE
    $1:C_EMAIL_ADDRESS LIKE '%.com%'
ORDER BY
    $1:C_FIRST_NAME;

SELECT
    $1:SR_RETURNED_DATE_SK,
    $1:SR_ITEM_SK,
    $1:SR_RETURN_QUANTITY
FROM
    snowlake_ext_table
WHERE
    $1:SR_RETURNED_DATE_SK = 2451964
ORDER BY
    $1:SR_RETURN_QUANTITY ASC;

    set used_cache_result = false;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT
    $1:SR_RETURNED_DATE_SK,
    $1:SR_RETURN_TIME_SK,
    $1:SR_ITEM_SK,
    $1:SR_NET_LOSS
FROM
    snowlake_ext_table
WHERE
    $1:SR_NET_LOSS < 1300
ORDER BY
    $1:SR_RETURN_QUANTITY;