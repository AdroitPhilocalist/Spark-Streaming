FROM apache/spark:4.0.1-scala2.13-java21-ubuntu

USER root

RUN mkdir -p /var/lib/apt/lists/partial && \
	apt-get update && \
	apt-get install -y python3 python3-distutils && \
	ln -sf /usr/bin/python3 /usr/bin/python && \
	apt-get clean && rm -rf /var/lib/apt/lists/*

ENV HOME=/opt/spark \
	SPARK_SUBMIT_OPTS="-Dspark.jars.ivy=/opt/spark/.ivy2 -Duser.home=/opt/spark"

RUN mkdir -p /opt/spark/.ivy2 /opt/spark/.ivy2.5.2 && chown -R spark:spark /opt/spark/.ivy2 /opt/spark/.ivy2.5.2

RUN mkdir -p /app && chown spark:spark /app

WORKDIR /app

COPY --chown=spark:spark network_stream_consumer.py .

USER spark

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1", "network_stream_consumer.py"]