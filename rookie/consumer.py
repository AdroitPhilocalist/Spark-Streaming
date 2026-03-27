from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaStructuredConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Step 2: Define the schema — must match what producer sent
schema = StructType([
    StructField("id",         IntegerType()),
    StructField("ip_address", StringType()),
    StructField("event_type", StringType()),
    StructField("status",     StringType()),
])

# Step 3: Read streaming data from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "structured-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Step 4: Kafka gives raw bytes — convert "value" column from JSON to columns
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")  # Expand JSON into proper columns

# Step 5: Write output to console (streaming)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/kafka-cp-new") \
    .start()

query.awaitTermination()