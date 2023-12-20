from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import countDistinct, col, when
from pyspark.sql.window import Window

#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
            .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
            .config("spark.executor.memory", "2G")  #excutor excute only 2G
            .config("spark.driver.memory","4G") 
            .config("spark.executor.cores","3") #Cluster use only 3 cores to excute
            .config("spark.python.worker.memory","1G") # each worker use 1G to excute
            .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
            .config("spark.kryoserializer.buffer.max","1024M")
            .getOrCreate())

#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile","service-account\lucky-wall-393304-2a6a3df38253.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

## Connect to the file in Google Bucket with Spark

user_path = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/extract/user_data.parquet"
post_path = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/extract/post_data.parquet"

df_user = spark.read.parquet(user_path)
df_post = spark.read.parquet(post_path)

## Explore the Data

print(df_user.show(5))
# Get the number of rows
num_rows_df_user = df_user.count()
# Get the number of columns
num_cols_df_user = len(df_user.columns)
# Print the shape
print("Shape of DataFrame: ({}, {})".format(num_rows_df_user, num_cols_df_user))
print("Info about User :")
# Print column names and their data types
print("Column Names and Data Types:")
for col_name, col_type in df_user.dtypes:
    print("{}: {}".format(col_name, col_type))
print("Number of unique User :")
for col_name in df_user.columns:
    unique_count = df_user.select(countDistinct(col(col_name))).collect()[0][0]
    print("{}: {}".format(col_name, unique_count))
num_duplicates_df_user = df_user.count() - df_user.dropDuplicates(df_user.columns).count()
print("Number of Duplicated Rows: {}".format(num_duplicates_df_user))
print("Number of Null Values for Each Column:")
for col_name in df_user.columns:
    null_count = df_user.filter(col(col_name).isNull()).count()
    print("{}: {}".format(col_name, null_count))

print(df_post.show(5))
# Get the number of rows
num_rows_df_post = df_post.count()
# Get the number of columns
num_cols_df_post = len(df_post.columns)
# Print the shape
print("Shape of DataFrame: ({}, {})".format(num_rows_df_post, num_cols_df_post))
print("Info about Post :")
# Print column names and their data types
print("Column Names and Data Types:")
for col_name, col_type in df_post.dtypes:
    print("{}: {}".format(col_name, col_type))
print("Number of unique Post :")
for col_name in df_post.columns:
    unique_count = df_post.select(countDistinct(col(col_name))).collect()[0][0]
    print("{}: {}".format(col_name, unique_count))
num_duplicates_df_post = df_post.count() - df_post.dropDuplicates(df_post.columns).count()
print("Number of Duplicated Rows: {}".format(num_duplicates_df_post))
print("Number of Null Values for Each Column:")
for col_name in df_post.columns:
    null_count = df_post.filter(col(col_name).isNull()).count()
    print("{}: {}".format(col_name, null_count))

# Handle Missing Values

# drop rows with any missing values
df_user = df_user.dropna()
df_post = df_post.dropna()

# Fill missing values in the specified column
df_user = df_user.na.fill('none', ["verified_type"])

# drop duplicate rows
df_user = df_user.dropDuplicates()
df_post = df_post.dropDuplicates()

# user drop column: fast_followers_count, is_translator, translator_type, want_retweets, protected
df_user = df_user.drop('fast_followers_count')
df_user = df_user.drop('is_translator')
df_user = df_user.drop('translator_type')
df_user = df_user.drop('want_retweets')
df_user = df_user.drop('protected')
# post drop column: lang
df_post = df_post.drop('lang')

# Sorting df_post by 'screen_name' and 'created_at'
window_spec = Window.partitionBy("screen_name").orderBy("created_at")
df_post = df_post.withColumn("time_diff", F.col("created_at").cast("long") - F.lag("created_at").over(window_spec))
average_time_diff = df_post.groupby('screen_name').agg(F.avg('time_diff').alias('average_time_diff'))
# Joining df_post with average_time_diff
df_post = df_post.join(average_time_diff, on='screen_name', how='left')

# Convert time_diff and average_time_diff to seconds
df_post = df_post.withColumn("time_diff_seconds", F.col("time_diff").cast("double"))
df_post = df_post.withColumn("average_time_diff_seconds", F.col("average_time_diff").cast("double"))

# Calculate absolute time difference in seconds and compare with the interval
df_post = df_post.withColumn("time_bot", (F.abs(F.col("time_diff_seconds") - F.col("average_time_diff_seconds")) < (5 * 60)).cast("int"))
# Grouping and aggregating for time_bot
time_bot_count = df_post.groupby('screen_name').agg(F.sum('time_bot').alias('time_bot_count'))

# Joining df_user with time_bot_count
df_user = df_user.join(time_bot_count, on='screen_name', how='left')
# Handling views column
df_post = df_post.withColumn("views", F.when(F.col("views") == "Unavailable", 0).otherwise(F.col("views").cast("int")))

# Creating views/like column
df_post = df_post.withColumn("views/like", F.when(F.col("favorite_count") != 0, F.col("views") / F.col("favorite_count")).otherwise(0))

# Filtering rows and creating views_score column
filtered_rows = (F.col("views/like").cast("float") > 50) & (F.col("views").cast("float") > 7000)
df_post = df_post.withColumn("views_score", F.when(filtered_rows, 1).otherwise(0))

# Grouping and aggregating for views_score
v_count = df_post.groupby('screen_name').agg(F.sum('views_score').alias('views_score'))

# Joining df_user with v_count
df_user = df_user.join(v_count, on='screen_name', how='left')

# Creating following/follower column
df_user = df_user.withColumn("following/follower", F.when(F.col("followers_count") != 0, F.col("friends_count") / F.col("followers_count")).otherwise(0))

# Creating friend_score column
filtered_rows = (F.col("following/follower").cast("float") > 1) & (F.col("friends_count").cast("int") > 500)
df_user = df_user.withColumn("friend_score", F.when(filtered_rows, 1).otherwise(0))

# Creating bot_score column
df_user = df_user.withColumn("bot_score", F.col("friend_score") + F.col("views_score") + F.col("time_bot_count"))

# Data Transformation
# convert 'timestamp' column to timestamp
df_post = df_post.withColumn('created_at', F.from_unixtime('created_at').cast('timestamp'))
df_user = df_user.withColumn('created_at', F.from_unixtime('created_at').cast('timestamp'))

# Summary statistics
summary_stats = df_user.summary()

# Get the 75th percentile value for 'bot_score'
percentile_75 = float(summary_stats.filter("summary = '75%'").select("bot_score").collect()[0]['bot_score'])

# Define conditions for updating 'verified_type'
condition1 = (col("verified_type") == "none") & (col("bot_score") >= percentile_75)
condition2 = (col("verified_type") == "none") & (col("bot_score") < percentile_75) & (col("verified") == False)
condition3 = (col("verified_type") == "none") & (col("bot_score") < percentile_75) & (col("verified") == True)

df_user = df_user.withColumn(
    "verified_type",
    when(condition1, "bot")
    .when(condition2, "default_user")
    .when(condition3, "KOL")
    .otherwise(col("verified_type"))
)

## Connect to the file in Google Bucket with Spark
user_transform_path = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/transformed-data/user_data_transform"
post_transform_path = f"gs://it4043e-it5384/it4043e/it4043e_group26_problem2/transformed-data/post_data_transform"

df_user.write.parquet(user_transform_path)
df_post.write.parquet(post_transform_path)

spark.stop()
