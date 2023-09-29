//External volume creation 
// Snowflake uses an external volume to establish a connection with your cloud storage in order to access Iceberg metadata and Parquet table data.
CREATE OR REPLACE EXTERNAL VOLUME snowlake_exvol
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'my-s3-us-east-1'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://snowlake-bucket/sales_data/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::745882220951:role/snowflake_role1'
                )  
      );

      
DESC EXTERNAL VOLUME SNOWLAKE_EXVOL;

      
//creation of the catalog integration   
//To create an Iceberg table that is not managed by Snowflake, you must specify a catalog integration. A catalog integration is an account-level Snowflake object that defines the source of metadata and schema for Iceberg tables.
CREATE OR REPLACE CATALOG INTEGRATION snowlake_glueCatalog
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='snowlake_db'
  TABLE_FORMAT=ICEBERG
  GLUE_CATALOG_ID='745882220951'
  GLUE_AWS_ROLE_ARN= 'arn:aws:iam::745882220951:role/service-role/AWSGlueServiceRole-snowlake'
  GLUE_REGION='us-east-1' 
  ENABLED=TRUE;


DESC CATALOG INTEGRATION snowlake_glueCatalog;


SHOW CATALOG INTEGRATIONS;

//creation of unmanaged iceberg table

CREATE or REPLACE ICEBERG TABLE iceberg_store_returns
  CATALOG_TABLE_NAME='iceberg_store_returns'
  CATALOG='snowlake_glueCatalog'
  EXTERNAL_VOLUME='SNOWLAKE_EXVOL';

select * from iceberg_store_returns limit 1000;


Alter iceberg table iceberg_store_returns refresh;


//to refresh the unmanaged iceberg table

Alter iceberg table iceberg_customer refresh;

CREATE or REPLACE ICEBERG TABLE iceberg_customer
  CATALOG_TABLE_NAME='iceberg_customer'
  CATALOG='snowlake_glueCatalog'
  EXTERNAL_VOLUME='SNOWLAKE_EXVOL';

select * from iceberg_customer LIMIT 10;



Drop iceberg table iceberg_customer;
//the location of the metadata file and status of the snapshot generation

Select SYSTEM$GET_ICEBERG_TABLE_INFORMATION('iceberg_store_returns');

// CREATING SECURE VIEW WITH   ICEBERG TABLE 
create or replace secure view SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_CUSTOMER_VIEW as
(select C_CUSTOMER_ID,C_CUSTOMER_SK,C_FIRST_NAME,C_LAST_NAME,C_BIRTH_COUNTRY,C_EMAIL_ADDRESS from snowlake_db.snowlake_unmanaged.iceberg_customer);

GRANT USAGE ON DATABASE SNOWLAKE_SHARE_DB TO SHARE CUSTOMER_DATA_SHARE;

GRANT USAGE ON SCHEMA SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE TO SHARE CUSTOMER_DATA_SHARE;

grant reference_usage on database SNOWLAKE_DB to share CUSTOMER_DATA_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_CUSTOMER_VIEW TO SHARE CUSTOMER_DATA_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.ICEBERG_CUSTOMER_VIEW TO SHARE CUSTOMER_DATA_SHARE;

ALTER SHARE CUSTOMER_DATA_SHARE ADD ACCOUNT = JZB03580;


// CREATING SECURE VIEW WITH TWO ICEBERG TABLES JOIN
--Join the customer table with the sales return table to identify high-value customers who have a history of returns
create or replace secure view iceberg_customer_store_view as  
(SELECT C.C_CUSTOMER_ID, C.C_FIRST_NAME, C.C_LAST_NAME, COUNT(R.SR_ITEM_SK) AS TOTAL_RETURNS
FROM  iceberg_customer c
LEFT JOIN iceberg_store_returns R ON C.C_CUSTOMER_SK = R.SR_CUSTOMER_SK
GROUP BY C.C_CUSTOMER_ID, C.C_FIRST_NAME, C.C_LAST_NAME
HAVING COUNT(R.SR_ITEM_SK) > 2
);
 
select * from iceberg_store_view limit 10 ;

CREATE OR REPLACE SHARE CUSTOMER_DATA_SHARE;

GRANT USAGE ON DATABASE SNOWLAKE_SHARE_DB TO SHARE CUSTOMER_DATA_SHARE;

GRANT USAGE ON SCHEMA SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE TO SHARE CUSTOMER_DATA_SHARE;

grant reference_usage on database SNOWLAKE_DB to share CUSTOMER_DATA_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.iceberg_customer_store_view TO SHARE CUSTOMER_DATA_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.iceberg_customer_store_view TO SHARE CUSTOMER_DATA_SHARE;

ALTER SHARE CUSTOMER_DATA_SHARE ADD ACCOUNT = JZB03580;

-- GRANT USAGE ON WAREHOUSE SNOWLAKE_WH TO SHARE STORE_DATA_SHARE;


// How to see share objects
SHOW SHARES;

// How to see the grants of a share object
SHOW GRANTS TO SHARE STORE_DATA_SHARE;


// Add the consumer account to share the data
ALTER SHARE STORE_DATA_SHARE ADD ACCOUNT = jzb03580;

// CREATING SECURE VIEW WITH ICEBERG AND NATIVE TABLE JOIN
create or replace secure view item_store_view as  
(SELECT
    I.I_ITEM_ID,
    I.I_ITEM_DESC,
    COUNT(R.SR_ITEM_SK) AS RETURN_COUNT
FROM SNOWLAKE_DB.SNOWLAKE_UNMANAGED.native_item_table I
LEFT JOIN SNOWLAKE_DB.SNOWLAKE_UNMANAGED.iceberg_store_returns R ON I.I_ITEM_SK = R.SR_ITEM_SK
GROUP BY I.I_ITEM_ID, I.I_ITEM_DESC
ORDER BY RETURN_COUNT DESC
);

select * from item_store_view limit 10;

GRANT USAGE ON DATABASE SNOWLAKE_SHARE_DB TO SHARE CUSTOMER_DATA_SHARE;

GRANT USAGE ON SCHEMA SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE TO SHARE CUSTOMER_DATA_SHARE;

grant reference_usage on database SNOWLAKE_DB to share CUSTOMER_DATA_SHARE;

GRANT SELECT ON VIEW SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.item_store_view TO SHARE CUSTOMER_DATA_SHARE;

GRANT SELECT ON TABLE SNOWLAKE_SHARE_DB.SNOWLAKE_SHARE.item_store_view TO SHARE CUSTOMER_DATA_SHARE;

ALTER SHARE CUSTOMER_DATA_SHARE ADD ACCOUNT = jzb03580;


//applying column level masking policy to unmanaged iceberg table 

create or replace masking policy mask_policy as (val string) returns string ->
	case
		when current_role() in ('ACCOUNTADMIN') then val
		else '*******'
	end;

    
alter iceberg table if exists iceberg_customer modify column C_EMAIL_ADDRESS set masking policy mask_policy;

alter iceberg table if exists iceberg_customer modify column C_EMAIL_ADDRESS unset masking policy;

select * from iceberg_customer limit 100;

//applying row level masking policy to unmanaged iceberg table


CREATE OR REPLACE ROW ACCESS POLICY row_masking_policy AS (c_birth_country STRING) RETURNS BOOLEAN ->
CASE 
     WHEN CURRENT_ROLE() = 'SNOWLAKE_ARL' AND C_BIRTH_COUNTRY= 'BAHRAIN' THEN TRUE
     ELSE FALSE 
END;

alter iceberg table ICEBERG_CUSTOMER add row access policy row_masking_policy on (c_birth_country);

alter iceberg table ICEBERG_CUSTOMER drop row access policy row_masking_policy;

select * from iceberg_customer limit 100;


show masking policies;


 
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

set used_cache_result = false;
//Performance metrics
SELECT
    C_FIRST_NAME,
    C_LAST_NAME,
    C_BIRTH_MONTH,
    C_BIRTH_YEAR
FROM
    iceberg_customer
WHERE
    C_BIRTH_MONTH = 4
    AND C_BIRTH_YEAR = 1985;

 

SELECT
    SR_RETURNED_DATE_SK,
    SR_RETURN_TIME_SK,
    SR_ITEM_SK,
    SR_NET_LOSS
FROM
    iceberg_store_returns
WHERE
    SR_NET_LOSS < 1300
ORDER BY
    SR_RETURN_QUANTITY;

SELECT
    C_FIRST_NAME,
    C_LAST_NAME,
    C_EMAIL_ADDRESS
FROM
    iceberg_customer
WHERE
    C_EMAIL_ADDRESS LIKE '%.com%'
ORDER BY
    C_FIRST_NAME;


SELECT
    SR_RETURNED_DATE_SK,
    SR_ITEM_SK,
    SR_RETURN_QUANTITY
FROM
    iceberg_store_returns
WHERE
    SR_RETURNED_DATE_SK = 2451981
ORDER BY
    SR_RETURN_QUANTITY ASC;





