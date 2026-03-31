from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("NetworkStreamConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema (must match producer)
schema = StructType([
    StructField("data", StructType([
        StructField("session", StructType([
            StructField("id", StringType())
        ])),
        StructField("network", StructType([
            StructField("src_ip",      StringType()),
            StructField("dst_ip",      StringType()),
            StructField("ip_protocol", StringType())
        ])),
        StructField("transport", StructType([
            StructField("src_port", IntegerType()),
            StructField("dst_port", IntegerType())
        ]))
    ]))
])




# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "network-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert JSON string → structured columns
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

# Then flatten directly:
final_df = parsed_df.select(
    col("data.data.session.id").alias("id"),
    col("data.data.network.src_ip").alias("src_ip"),
    col("data.data.network.dst_ip").alias("dst_ip"),
    col("data.data.network.ip_protocol").alias("ip_protocol"),
    col("data.data.transport.src_port").alias("src_port"),
    col("data.data.transport.dst_port").alias("dst_port")
)

# Output
query = final_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/network-stream-cp") \
    .start()

query.awaitTermination()