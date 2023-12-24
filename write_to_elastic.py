from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv
load_dotenv()

SPARK_MASTER = os.getenv("SPARK_MASTER")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")
#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master(SPARK_MASTER)
            .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
            .config("spark.executor.memory", "2G")  #excutor excute only 2G
            .config("spark.driver.memory","4G") 
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.3")
            .config("spark.executor.cores","1") #Cluster use only 3 cores to excute
            .config("spark.python.worker.memory","1G") # each worker use 1G to excute
            .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
            .config("spark.kryoserializer.buffer.max","1024M")
            .getOrCreate())

#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

## Connect to the file in Google Bucket with Spark

user_path = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/transformed-data/user_data_cleaned/part-00000-4fd9f0df-b6a4-4e5a-8acc-f3a1a8292658-c000.snappy.parquet"
post_path_1 = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/transformed-data/post_data_cleaned/part-00000-b90bc710-f588-40a7-9619-539cbe8d6e85-c000.snappy.parquet"
post_path_2 = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/transformed-data/post_data_cleaned/part-00001-b90bc710-f588-40a7-9619-539cbe8d6e85-c000.snappy.parquet"

df_user = spark.read.parquet(user_path)
df_post_1 = spark.read.parquet(post_path_1)
df_post_2 = spark.read.parquet(post_path_2)
# Add row numbers to df_post_2 using zipWithIndex
df_post_2 = df_post_2.withColumn("row_num", F.monotonically_increasing_id())
df_post_2 = df_post_2.withColumn("row_num", F.col("row_num") - 1)  # Adjust row numbers to start from 0

# Exclude the first row
df_post_2 = df_post_2.filter("row_num > 0").drop("row_num")
# Find common columns
common_columns = list(set(df_post_2.columns) & set(df_post_2.columns))
# Select only the common columns and concatenate the DataFrames
concatenated_df_post = df_post_1.select(common_columns).union(df_post_2.select(common_columns))

# Define Elasticsearch index name
elasticsearch_index_user = "group26_user"
elasticsearch_index_post = "group26_post"

# Configure Elasticsearch settings
es_write_conf_user = {
    "es.nodes": f"{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}",
    "es.resource": f"{elasticsearch_index_user}/_doc",
    "es.input.json": "yes"
}
es_write_conf_post = {
    "es.nodes": f"{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}",
    "es.resource": f"{elasticsearch_index_post}/_doc",
    "es.input.json": "yes"
}

# Write JSON data to Elasticsearch
df_user.write.format("org.elasticsearch.spark.sql").options(**es_write_conf_user).mode("append").save()
concatenated_df_post.write.format("org.elasticsearch.spark.sql").options(**es_write_conf_post).mode("append").save()

print("Complete")
# Stop Spark session
spark.stop()