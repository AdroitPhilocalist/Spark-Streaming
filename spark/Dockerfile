FROM apache/spark:3.5.1

USER root

# Install kafka connector for Spark
RUN pip install pyspark==3.5.0

# Copy your scripts
COPY producer.py /app/producer.py
COPY consumer.py /app/consumer.py

WORKDIR /app

# Default: run the consumer (streaming mode)
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "consumer.py"]