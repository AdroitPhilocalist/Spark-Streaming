from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
import time

spark= SparkSession.builder.appName("KafkaStructuredProducer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "192.168.1.1", "LOGIN_ATTEMPT", "FAILED"),
    (2, "192.168.1.2", "FILE_ACCESS",   "SUCCESS"),
    (3, "10.0.0.5",    "PORT_SCAN",     "DETECTED"),
    (4, "172.16.0.3",  "LOGIN_ATTEMPT", "SUCCESS"),
    (5, "10.0.0.5",    "BRUTE_FORCE",   "DETECTED"),
]

columns = ["id", "ip_address", "event_type", "status"]
df = spark.createDataFrame(data, columns)
# Step 3: Convert each row to JSON and send to Kafka
# Kafka expects a "value" column — we pack all columns into JSON
kafka_df = df.select(
    to_json(struct([col(c) for c in df.columns])).alias("value")
)

# Step 4: Write to Kafka topic (batch write)
while True:
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "structured-topic") \
        .save()

    time.sleep(2)

print("Data successfully written to Kafka topic: structured-topic")
spark.stop()