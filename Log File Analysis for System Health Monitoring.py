from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, expr, to_date,to_timestamp

spark=(
    SparkSession
    .builder
    .appName("Log File Analysis for System Health Monitoring")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.option("multiline", "true").json("D:\\spark-app\\sample_logs.json")
                     
df.printSchema()
df.show(truncate=False)

# Task 1: Identify top 3 servers with highest ERROR log
top_error_servers = df.filter(col("log_level") == 'ERROR') \
    .groupBy("server_id") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(3)

print("Top 3 Servers with Highest ERROR Logs:")
top_error_servers.show()

# Task 2: Calculate average number of logs generated per day by each server
average_logs_per_day = df.groupBy("server_id") \
    .agg(count("*").alias("total_logs")) \
    .withColumn("average_per_day", col("total_logs") / 7)  # Assuming data is for 7 days

print("Average Number of Logs Generated Per Day by Each Server:")
average_logs_per_day.show()

# Task 3: Summary report of most common log messages for each severity level
common_messages_summary = df.groupBy("log_level", "message") \
    .agg(count("*").alias("message_count")) \
    .orderBy(desc("message_count"))

print("Summary Report of Most Common Log Messages for Each Severity Level:")
common_messages_summary.show()