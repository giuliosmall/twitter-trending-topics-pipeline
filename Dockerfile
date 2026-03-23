# Use the official PySpark base image that supports both ARM and x86 architectures
FROM apache/spark-py:v3.2.1

# Set environment variables for Python and Spark paths
ENV PATH="/home/sparkuser/.local/bin:${PATH}"
ENV PYSPARK_PYTHON="/usr/bin/python3"
ENV PYSPARK_DRIVER_PYTHON="/usr/bin/python3"
ENV USE_MINIO="true"

# Switch to root user to avoid permission issues
USER root

# Create directories and set permissions
RUN mkdir -p /home/sparkuser/app && mkdir -p /home/sparkuser/data && mkdir -p /home/sparkuser/output && chmod -R 777 /home/sparkuser

WORKDIR /home/sparkuser/app

# --- Dependencies layer (cached unless Dockerfile changes) ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    openjdk-11-jdk \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.2.1 boto3 minio "spacy>=3.7,<3.8" \
    stop-words pytest streamlit wordcloud matplotlib

RUN python3 -m spacy download en_core_web_md && \
    python3 -m spacy download nl_core_news_md

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P /opt/spark/jars

# --- Application code (changes frequently, cached layers above are preserved) ---
COPY app/ /home/sparkuser/app/

EXPOSE 4040 8080 8081 7077 6066 9000 9001 8501

USER root

CMD ["/bin/bash"]
