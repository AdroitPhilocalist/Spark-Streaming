from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("JSONBatch").getOrCreate()

df = spark.read.json("1672391800_00000887_0000.json")

# df.printSchema()
# df.show(1, truncate=False)



df_selected = df.select(
    col("session.id"),
    col("network.src_ip"),
    col("network.dst_ip"),
    col("network.ip_protocol"),
    col("transport.src_port"),
    col("transport.dst_port")
)

# df_selected.show()

from pyspark.sql.functions import to_json, struct, col

kafka_df = df_selected.select(
    to_json(struct("*")).alias("value")
)
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "network-topic") \
    .save()