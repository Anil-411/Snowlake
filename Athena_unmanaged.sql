Athena:

//Iceberg table creation:

create table iceberg_customer(
c_customer_sk decimal,
c_customer_id string ,
c_current_cdemo_sk decimal,
c_current_hdemo_sk decimal,
c_current_addr_sk decimal,
c_first_shipto_date_sk decimal,
c_first_sales_date_sk decimal,
c_salutation string,
c_first_name string,
c_last_name string,
c_preferred_cust_flag string,
c_birth_day decimal,
c_birth_month decimal,
c_birth_year decimal,
c_birth_country string,
c_login string,
c_email_address string,
c_last_review_date string)
LOCATION 's3://snowlake-bucket/salesdata/iceberg_customer'
tblproperties('table_type'='ICEBERG')

// inserting into iceberg table using catalog

insert into iceberg_customer  (select * from customer)

// Schema Evolution  in Athena

ALTER TABLE my_db.my_table RENAME TO my_db2.my_table2
 
ALTER TABLE iceberg_customer ADD COLUMNS (comment string)
 
ALTER TABLE iceberg_customer CHANGE comment blog_comment string AFTER id
  
ALTER TABLE iceberg_customer DROP COLUMN comment

insert into iceberg_customer(c_customer_sk) values(12112211)

delete from iceberg_customer where c_customer_sk=12112211