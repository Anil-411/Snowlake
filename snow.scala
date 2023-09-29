spark-shell --packages org.apache.spark:spark-core_2.12:3.3.2,org.apache.spark:spark-sql_2.12:3.3.2,org.apache.iceberg:iceberg-spark3:0.13.1,org.apache.logging.log4j:log4j-core:2.20.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-glue:1.12.471,com.amazonaws:aws-java-sdk-s3:1.12.544,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.14.1,org.apache.hive:hive-metastore:3.1.3  

import org.apache.spark.sql._  

import org.apache.spark.SparkConf  

val sparkConf = new SparkConf().setAppName("YourSparkApp").setMaster("local[*]").set("spark.driver.memory", "2g")  

val spark = SparkSession.builder().config(sparkConf) .master("local[3]").config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.local.type", "hadoop").config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com").config("spark.hadoop.fs.s3a.region", "us-east-1").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", "AKIA23KQLPGLSRCTCPOS").config("spark.hadoop.fs.s3a.secret.key", "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").appName("iceberg-spark").getOrCreate()  

val snowflakeOptions = Map(                
"sfURL" -> "https://anblicksorg_aws.us-east-1.snowflakecomputing.com",               
"sfUser" -> "SNOWLAKE",                
"sfpassword" -> "SnowLake@202308",                
"sfDatabase" -> "SNOWLAKE_DB",          
"sfSchema" -> "SNOWFLAKE_MANAGED",                
"sfWarehouse" -> "SNOWLAKE_WH",                 
"sfRole" -> "SNOWLAKE_ARL"             
 )  

val ff = spark.read.format("net.snowflake.spark.snowflake").options(snowflakeOptions).option("dbtable", "SNOWLAKE_DB.SNOWFLAKE_MANAGED.iceberg_managed_store_returns").load("s3://snowlake-bucket/managed-iceberg/12325538234778022/").createOrReplaceTempView("iceberg_returns");  

spark.sql("SELECT count(*) from iceberg_returns").show(false)   

spark.sql("SELECT SR_RETURNED_DATE_SK,SR_RETURN_TIME_SK,SR_ITEM_SK from iceberg_returns where  SR_RETURNED_DATE_SK IN ('24516134', '24516135') ").show(false)  