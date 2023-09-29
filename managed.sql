--CREATING SCHEMA

create or replace schema snowflake_managed


--CREATING EXTERNAL VOLUME

--Snowflake uses an external volume to establish a connection with your cloud storage in order to access Iceberg metadata and Parquet table data.

CREATE OR REPLACE EXTERNAL VOLUME snowlake_managed_exvol
STORAGE_LOCATIONS =
  (
     (
        NAME = 'my-s3-us-east-1'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://snowlake-bucket/managed-iceberg/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::745882220951:role/snowlake-managed-iceberg-role'
            )
  );


show external volumes

--The trust relationship must be configured separately external volume .
--Need to add External ID generated from External volume in the IAM role snowlake-managed-iceberg-role to access the locations.

--DESCRIBING THE EXTERNAL VOLUME

DESC EXTERNAL VOLUME snowlake_managed_exvol;

--CREATING STORAGE INTEGRATION

create or replace storage integration snowlake_managed_integration
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::745882220951:role/store-role'
    enabled = true
    storage_allowed_locations = ('s3://snowlake-bucket/salesdata/store_returns')


--DESCRIBING STORAGE INTEGRATION
    
DESC INTEGRATION snowlake_managed_integration;

--drop INTEGRATION  s3_integration

show integrations;
--CREATING EXTERNAL STAGE
create or replace stage external_stage
url='s3://snowlake-bucket/salesdata/store_returns'
storage_integration=snowlake_managed_integration;


list @external_stage;


--CREATING FILE FORMAT

-create or replace file format iceberg_format type='parquet' COMPRESSION = 'AUTO';

-- show tables in account

show iceberg tables
-- drop table iceberg_sales


--CREATION OF ICEBERG TABLE

CREATE or REPLACE ICEBERG TABLE iceberg_managed_store_returns
(
SR_RETURNED_DATE_SK decimal,
SR_RETURN_TIME_SK decimal,
SR_ITEM_SK decimal,
SR_CUSTOMER_SK decimal,
SR_CDEMO_SK decimal,
SR_HDEMO_SK decimal,
SR_ADDR_SK decimal,
SR_STORE_SK decimal,
SR_REASON_SK decimal,
SR_TICKET_NUMBER decimal,
SR_RETURN_QUANTITY decimal,
SR_RETURN_AMT decimal,
SR_RETURN_TAX decimal,
SR_RETURN_AMT_INC_TAX decimal,
SR_FEE decimal,
SR_RETURN_SHIP_COST decimal,
SR_REFUNDED_CASH decimal,
SR_REVERSED_CHARGE decimal,
SR_STORE_CREDIT decimal,
SR_NET_LOSS decimal
) 
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='snowlake_managed_exvol'
BASE_LOCATION='managed-iceberg/'
as
select
$1:SR_RETURNED_DATE_SK:: decimal as SR_RETURNED_DATE_SK
,$1:SR_RETURN_TIME_SK::decimal as SR_RETURN_TIME_SK
,$1:SR_ITEM_SK::decimal as SR_ITEM_SK
,$1:SR_CUSTOMER_SK::decimal as SR_CUSTOMER_SK
,$1:SR_CDEMO_SK::decimal as SR_CDEMO_SK
,$1:SR_HDEMO_SK::decimal as SR_HDEMO_SK
,$1:SR_ADDR_SK::decimal as SR_ADDR_SK
,$1:SR_STORE_SK::decimal as SR_STORE_SK
,$1:SR_REASON_SK::decimal as SR_REASON_SK
,$1:SR_TICKET_NUMBER::decimal as SR_TICKET_NUMBER
,$1:SR_RETURN_QUANTITY::decimal as SR_RETURN_QUANTITY
,$1:SR_RETURN_AMT::decimal as SR_RETURN_AMT 
,$1:SR_RETURN_TAX::decimal as SR_RETURN_TAX
,$1:SR_RETURN_AMT_INC_TAX::decimal as SR_RETURN_AMT_INC_TAX
,$1:SR_FEE::decimal as SR_FEE
,$1:SR_RETURN_SHIP_COST::decimal as SR_RETURN_SHIP_COST
,$1:SR_REFUNDED_CASH::decimal as SR_REFUNDED_CASH
,$1:SR_REVERSED_CHARGE::decimal as SR_REVERSED_CHARGE
,$1:SR_STORE_CREDIT::decimal as SR_STORE_CREDIT 
,$1:SR_NET_LOSS::decimal as SR_NET_LOSS
from @external_stage (file_format => 'iceberg_format');


SELECT COUNT(*) FROM iceberg_managed_store_returns; -- Initial count = 2879898629

--auto ingest

create or replace pipe managed_snowpipe
auto_ingest= true as
copy into iceberg_managed_store_returns
from(select
$1:SR_RETURNED_DATE_SK:: decimal as SR_RETURNED_DATE_SK
,$1:SR_RETURN_TIME_SK::decimal as SR_RETURN_TIME_SK
,$1:SR_ITEM_SK::decimal as SR_ITEM_SK
,$1:SR_CUSTOMER_SK::decimal as SR_CUSTOMER_SK
,$1:SR_CDEMO_SK::decimal as SR_CDEMO_SK
,$1:SR_HDEMO_SK::decimal as SR_HDEMO_SK
,$1:SR_ADDR_SK::decimal as SR_ADDR_SK
,$1:SR_STORE_SK::decimal as SR_STORE_SK
,$1:SR_REASON_SK::decimal as SR_REASON_SK
,$1:SR_TICKET_NUMBER::decimal as SR_TICKET_NUMBER
,$1:SR_RETURN_QUANTITY::decimal as SR_RETURN_QUANTITY
,$1:SR_RETURN_AMT::decimal as SR_RETURN_AMT 
,$1:SR_RETURN_TAX::decimal as SR_RETURN_TAX
,$1:SR_RETURN_AMT_INC_TAX::decimal as SR_RETURN_AMT_INC_TAX
,$1:SR_FEE::decimal as SR_FEE
,$1:SR_RETURN_SHIP_COST::decimal as SR_RETURN_SHIP_COST
,$1:SR_REFUNDED_CASH::decimal as SR_REFUNDED_CASH
,$1:SR_REVERSED_CHARGE::decimal as SR_REVERSED_CHARGE
,$1:SR_STORE_CREDIT::decimal as SR_STORE_CREDIT 
,$1:SR_NET_LOSS::decimal as SR_NET_LOSS
from @external_stage (file_format => 'iceberg_format'));

select count(*) from iceberg_managed_store_returns; --after incremental load count = 2879931141 , increased by 325132,2879963653

show pipes;

desc pipe managed_snowpipe

--drop pipe snowpipe

--Generating Iceberg metadata files and updating them to reflect DML changes to a table requires calling the SYSTEM$GET_ICEBERG_TABLE_INFORMATION function for tables that are managed by Snowflake.
--To sync metadata for other systems to access the recent version, creates metadata snapshot in s3

select SYSTEM$GET_ICEBERG_TABLE_INFORMATION('iceberg_managed_store_returns');
--"s3://snowlake-bucket/managed-iceberg/12325538234778022/metadata/v1.metadata.json","status":"success"



create or replace secure view SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_MANAGED_VIEW as
(select I_ITEM_SK,I_ITEM_ID,I_ITEM_DESC from snowlake_db.snowFlake_managed.MANAGED_ITEMS);-- one iceberg table

create or replace secure view sales_info as
select * from SNOWLAKE_DB.SNOWFLAKE_MANAGED.ICEBERG_MANAGED_STORE_RETURNS st join SNOWLAKE_DB.SNOWFLAKE_MANAGED.MANAGED_ITEMS item on item.I_ITEM_SK= st.SR_ITEM_SK;--2 iceberg tables

create or replace secure view iceberg_native_info as
select * from SNOWLAKE_DB.SNOWFLAKE_MANAGED.native_store_returns nt join SNOWLAKE_DB.SNOWFLAKE_MANAGED.MANAGED_ITEMS item on item.I_ITEM_SK= nt.SR_ITEM_SK; -- iceberg table and native table

select * from SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_MANAGED_VIEW;

create or replace share MANAGED_SHARE;

GRANT USAGE ON DATABASE SNOWLAKE_SHARE_DB TO SHARE MANAGED_SHARE;

GRANT USAGE ON SCHEMA SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE TO SHARE MANAGED_SHARE;

grant reference_usage on database SNOWLAKE_DB to share MANAGED_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_MANAGED_VIEW TO SHARE MANAGED_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_MANAGED_VIEW TO SHARE MANAGED_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.sales_info TO SHARE MANAGED_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.sales_info TO SHARE MANAGED_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.iceberg_native_info TO SHARE MANAGED_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.iceberg_native_info TO SHARE MANAGED_SHARE;

ALTER SHARE MANAGED_SHARE ADD ACCOUNT = jzb03580;



--Schema evolution

Alter iceberg table iceberg_managed_store_returns RENAME column SR_REFUNDED_CASH to SR_REFUNDED_MONEY;
Alter iceberg table iceberg_managed_store_returns ADD column SR_REPRESENTATIVE varchar;
Alter iceberg table iceberg_managed_store_returns DROP column SR_REPRESENTATIVE;

select * from iceberg_managed_store_returns limit 10;


INSERT INTO iceberg_managed_store_returns (SR_RETURNED_DATE_SK ,SR_RETURN_TIME_SK,SR_ITEM_SK)
    VALUES ('24516134', '67000', '18888'),
           ('24516135', '70000', '19999');

UPDATE iceberg_managed_store_returns SET SR_CUSTOMER_SK = 3456778 WHERE SR_RETURNED_DATE_SK=24516134;
UPDATE iceberg_managed_store_returns SET SR_CUSTOMER_SK =123  WHERE SR_RETURNED_DATE_SK=24516134;
    
select COUNT(*) from iceberg_managed_store_returns limit 10; --2879931143

select SYSTEM$GET_ICEBERG_TABLE_INFORMATION('iceberg_managed_store_returns');
--{"metadataLocation":"s3://snowlake-bucket/managed-iceberg/12325538234778022/metadata/v2.metadata.json","status":"success"}


DELETE FROM iceberg_managed_store_returns WHERE SR_RETURNED_DATE_SK IN ('24516134', '24516135');
--Creating Iceberg table for customer data

CREATE OR REPLACE iceberg TABLE managed_customer
(
    C_CUSTOMER_SK number,
    C_CUSTOMER_ID varchar,
    C_CURRENT_CDEMO_SK number,
    C_CURRENT_HDEMO_SK number,
    C_CURRENT_ADDR_SK number,
    C_FIRST_SHIPTO_DATE_SK number,
    C_FIRST_SALES_DATE_SK number,
    C_SALUTATION varchar,
    C_FIRST_NAME varchar,
    C_LAST_NAME varchar,
    C_PREFERRED_CUST_FLAG varchar,
    C_BIRTH_DAY number,
    C_BIRTH_MONTH number,
    C_BIRTH_YEAR number,
    C_BIRTH_COUNTRY varchar,
    C_LOGIN varchar,
    C_EMAIL_ADDRESS varchar,
    C_LAST_REVIEW_DATE varchar
) 
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='snowlake_managed_exvol'
BASE_LOCATION='managed-iceberg/'
AS 
SELECT
$1:"C_CUSTOMER_SK"::number as C_CUSTOMER_SK
,$1:"C_CUSTOMER_ID"::varchar as C_CUSTOMER_ID
,$1:"C_CURRENT_CDEMO_SK"::number as C_CURRENT_CDEMO_SK
,$1:"C_CURRENT_HDEMO_SK"::number as C_CURRENT_HDEMO_SK
,$1:"C_CURRENT_ADDR_SK"::number as C_CURRENT_ADDR_SK
,$1:"C_FIRST_SHIPTO_DATE_SK"::number as C_FIRST_SHIPTO_DATE_SK
,$1:"C_FIRST_SALES_DATE_SK"::number as C_FIRST_SALES_DATE_SK
,$1:"C_SALUTATION"::varchar as C_SALUTATION
,$1:"C_FIRST_NAME"::varchar as C_FIRST_NAME
,$1:"C_LAST_NAME"::varchar as C_LAST_NAME
,$1:"C_PREFERRED_CUST_FLAG"::varchar as C_PREFERRED_CUST_FLAG
,$1:"C_BIRTH_DAY"::decimal as C_BIRTH_DAY
,$1:"C_BIRTH_MONTH"::decimal as C_BIRTH_MONTH
,$1:"C_BIRTH_YEAR"::decimal as C_BIRTH_YEAR
,$1:"C_BIRTH_COUNTRY"::varchar as C_BIRTH_COUNTRY
,$1:"C_LOGIN"::varchar as C_LOGIN
,$1:"C_EMAIL_ADDRESS"::varchar as C_EMAIL_ADDRESS
,$1:"C_LAST_REVIEW_DATE"::varchar as C_LAST_REVIEW_DATE
FROM @cus_stage(FILE_FORMAT => 'iceberg_format');


select * from managed_customer



--Creating Iceberg table for item data

CREATE or REPLACE iceberg  TABLE managed_items(
I_ITEM_SK NUMBER,
I_ITEM_ID VARCHAR,
I_REC_START_DATE DATE,
I_REC_END_DATE DATE,
I_ITEM_DESC VARCHAR,
I_CURRENT_PRICE NUMBER,
I_WHOLESALE_COST NUMBER,
I_BRAND_ID NUMBER,
I_BRAND VARCHAR,
I_CLASS_ID NUMBER,
I_CLASS VARCHAR,
I_CATEGORY_ID NUMBER,
I_CATEGORY VARCHAR,
I_MANUFACT_ID NUMBER,
I_MANUFACT VARCHAR,
I_SIZE VARCHAR,
I_FORMULATION VARCHAR,
I_COLOR VARCHAR,
I_UNITS VARCHAR,
I_CONTAINER VARCHAR,
I_MANAGER_ID NUMBER,
I_PRODUCT_NAME VARCHAR
)
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='snowlake_managed_exvol'
BASE_LOCATION='managed-iceberg/'
as
select
$1:I_ITEM_SK:: NUMBER as I_ITEM_SK
,$1:I_ITEM_ID:: VARCHAR as I_ITEM_ID
,$1:I_REC_START_DATE::DATE as I_REC_START_DATE
,$1:I_REC_END_DATE::DATE as I_REC_END_DATE
,$1:I_ITEM_DESC::VARCHAR as I_ITEM_DESC
,$1:I_CURRENT_PRICE::NUMBER as I_CURRENT_PRICE
,$1:I_WHOLESALE_COST::NUMBER as I_WHOLESALE_COST
,$1:I_BRAND_ID::NUMBER as I_BRAND_ID
,$1:I_BRAND::VARCHAR as I_BRAND
,$1:I_CLASS_ID::NUMBER as I_CLASS_ID
,$1:I_CLASS::VARCHAR as I_CLASS
,$1:I_CATEGORY_ID::NUMBER as I_CATEGORY_ID
,$1:I_CATEGORY::VARCHAR as I_CATEGORY
,$1:I_MANUFACT_ID::NUMBER as I_MANUFACT_ID
,$1:I_MANUFACT::VARCHAR as I_MANUFACT
,$1:I_SIZE::VARCHAR as I_SIZE
,$1:I_FORMULATION::VARCHAR as I_FORMULATION
,$1:I_COLOR::VARCHAR as I_COLOR
,$1:I_UNITS::VARCHAR as I_UNITS
,$1:I_CONTAINER::VARCHAR as I_CONTAINER
,$1:I_MANAGER_ID::NUMBER as I_MANAGER_ID
,$1:I_PRODUCT_NAME::VARCHAR as I_PRODUCT_NAME
FROM @item_stage(FILE_FORMAT => 'iceberg_format') ;


--DATA MASKING POLICY ON CUSTOMERS 
create or replace masking policy customer_mask as (val string) returns string ->
	case
		when current_role() in ('ACCOUNTADMIN') then val
		else '**HIDDEN**'
	end;
    
alter iceberg table if exists managed_customer modify column C_EMAIL_ADDRESS set masking policy customer_mask;

--alter iceberg table if exists managed_customer modify column C_EMAIL_ADDRESS unset masking policy;

show masking policies;

select * from managed_customer;


--ALTER iceberg TABLE managed_customer modify column C_EMAIL_ADDRESS unset masking policy;


--ROW ACCESS POLICY ON ITEMS

create or replace row access policy item_category 
    as (I_CATEGORY string) returns boolean ->
    case   when 'ACCOUNTADMIN' = current_role() then true
       when 'SNOWLAKE_ARL' = current_role() and I_CATEGORY = 'Men' then true
       
    else false
end

alter iceberg table MANAGED_ITEMS add row access policy item_category on (I_CATEGORY);


SELECT * FROM MANAGED_ITEMS;


--alter iceberg table MANAGED_ITEMS drop row access policy item_category;

--Multi table transactions 

BEGIN; 
DELETE FROM native_items 	WHERE I_CLASS = 'pop' and I_CATEGORY = 'Music'; 
DELETE FROM multi_items 	WHERE I_CLASS = 'pop' and I_CATEGORY = 'Music'; 
COMMIT;

 

select count(*) from native_items
select * from native_items WHERE I_CATEGORY = 'Music'
and I_CATEGORY = 'Music';







ALTER SESSION SET USE_CACHED_RESULT = FALSE;

set used_cache_result = false;
//performance metrics
SELECT
    SR_RETURNED_DATE_SK,
    SR_RETURN_TIME_SK,
    SR_ITEM_SK,
    SR_NET_LOSS
FROM
    iceberg_managed_store_returns
WHERE
    SR_NET_LOSS < 1300
ORDER BY
    SR_RETURN_QUANTITY;


SELECT
    C_FIRST_NAME,
    C_LAST_NAME,
    C_BIRTH_MONTH,
    C_BIRTH_YEAR
FROM
    managed_customer
WHERE
    C_BIRTH_MONTH = 4
    AND C_BIRTH_YEAR = 1985;

SELECT
    C_FIRST_NAME,
    C_LAST_NAME,
    C_SALUTATION,
    C_EMAIL_ADDRESS
FROM
    managed_customer
WHERE
    C_SALUTATION = 'Mr.'
    AND C_EMAIL_ADDRESS IS NOT NULL
ORDER BY
    C_LAST_NAME;



SELECT
    C_FIRST_NAME,
    C_LAST_NAME,
    C_EMAIL_ADDRESS
FROM
    managed_customer
WHERE
    C_EMAIL_ADDRESS LIKE '%.com%'
ORDER BY
    C_FIRST_NAME;

SELECT
    C_FIRST_NAME,
    C_LAST_NAME
FROM
    managed_customer
WHERE
    C_PREFERRED_CUST_FLAG = 'Y'
ORDER BY
    C_FIRST_NAME;



SELECT
    SR_RETURNED_DATE_SK,
    SR_ITEM_SK,
    SR_RETURN_QUANTITY
FROM
    iceberg_managed_store_returns
WHERE
    SR_RETURNED_DATE_SK = 2451981
ORDER BY
    SR_RETURN_QUANTITY ASC;






SELECT
    SR_RETURNED_DATE_SK,
    SR_ITEM_SK,
    SR_RETURN_QUANTITY
FROM
    iceberg_managed_store_returns
WHERE
    SR_STORE_SK < 100
ORDER BY
    SR_RETURN_QUANTITY DESC;