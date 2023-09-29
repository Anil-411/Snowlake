Title:

SnowLake - Leveraging Iceberg Tables in Snowflake Data Cloud

​Description:

Design and implement a proof of concept that leverages Iceberg Tables in Snowflake Data Cloud.​​

1. Implement Snowflake Unmanaged Iceberg Tables​​
2. Implement Snowflake Managed Iceberg Tables​​


Prerequisites:
 --Snowflake Account(Iceberg feature enabled)
 --AWS Account
 --SPARK

Getting Started:
 1.Snowflake setup
    - create snowflake account with necessary roles,databases and warehouse for your project
 2.AWS setup
    - Bucket creation , AWS Glue Catalog, AWS Athena and necessary roles and policies for S3 and Glue
 3.Spark setup
    - Add necessary dependencies as mentioned in  .scala file

For managed iceberg table creation follow managed.sql file
For unmanaged iceberg table creation follow unmanaged.sql file

For more details refer
https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/tables-iceberg