import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, from_json, col, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, StringType, IntegerType, DateType, TimestampType
import time
from kafka import KafkaConsumer
from pyspark.sql.functions import *

scala_version = '2.12'
spark_version = '3.3.2'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.2'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example1")\
   .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("building spark session")

kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "crypto_test_streaming_topic")\
        .option("startingOffsets", "latest") \
        .load()
#print(kafka_df.columns)

print("building kafka_df")

print(kafka_df)
#print(kafka_df.schema)
json_schema = StructType([
    StructField("Symbol", StringType()),
    StructField("price_USD", DoubleType()),
    StructField("24h_change%", DoubleType()),
    StructField("1h_change%", DoubleType()),
    StructField("last_updated", TimestampType())
    ])

print("Printing the first row ########### \n\n\n\n ###############")
df = kafka_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"),json_schema).alias("data"))\
        .select("data.*")

#print(col("value"))
print("building output_df")

#print(output_df)
#agg_df = df.groupBy("Symbol").agg(count("*").alias("count"))
agg_df = df.groupBy("Symbol").agg(avg("price_USD").alias("average"))
output_query = agg_df \
        .writeStream \
        .format("console") \
        .outputMode("complete")\
        .start()
print("final step")
output_query.awaitTermination()
