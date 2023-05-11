from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_json, from_json, col, count, sum, window, expr, desc, explode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
scala_version = '2.12'
spark_version = '3.1.2'

# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.7.0'
]

spark = SparkSession.builder \
    .appName('kafka-aggregations-pivot') \
    .config('spark.sql.shuffle.partitions', '2') \
    .config('spark.streaming.stopGracefullyOnShutdown', 'true') \
    .config('spark.sql.streaming.checkpointLocation', 'checkpoint') \
    .config('spark.jars.packages', ','.join(packages)) \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'crypto_test_streaming_topic') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

json_schema = StructType([
    StructField('Symbol', StringType()),
    StructField('price_USD', DoubleType()),
    StructField('24h_change%', DoubleType()),
    StructField('1h_change%', DoubleType()),
    StructField('last_updated', TimestampType())
])

df = kafka_df.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), json_schema).alias('data')) \
    .select('data.*')

agg_df = df.groupBy('Symbol').agg(count('*').alias('count'))

output_query = agg_df.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .foreachBatch(lambda df, epoch_id: df.groupBy().pivot('Symbol').agg(count("*").alias("count")).write.format('console').save()) \
    .start()

output_query.awaitTermination()
