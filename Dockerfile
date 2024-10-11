# Use the official PySpark base image that supports both ARM and x86 architectures
FROM apache/spark-py:v3.2.1

# Set environment variables for Python and Spark paths
ENV PATH="/home/sparkuser/.local/bin:${PATH}"
ENV PYSPARK_PYTHON="/usr/bin/python3"
ENV PYSPARK_DRIVER_PYTHON="/usr/bin/python3"
ENV USE_MINIO="true"

# Switch to root user to avoid permission issues
USER root

# Create a directory for the Spark user and set permissions
RUN mkdir -p /home/sparkuser/app && mkdir -p /home/sparkuser/data && mkdir -p /home/sparkuser/output && chmod -R 777 /home/sparkuser

# Copy the application files to the image
COPY app/ /home/sparkuser/app/
WORKDIR /home/sparkuser/app

# Install packages and JARs
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    openjdk-11-jdk \
    python3-pip && \
    pip install --no-cache-dir pyspark==3.2.1 boto3 minio spacy stop-words pytest && \
    python3 -m spacy download en_core_web_sm && \
    python3 -m spacy download nl_core_news_sm && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P /opt/spark/jars && \
    rm -rf /var/lib/apt/lists/*

# Dashboarding packages
RUN pip install streamlit wordcloud matplotlib

# Expose Spark UI and Worker ports along with Streamlit's port
EXPOSE 4040 8080 8081 7077 6066 9000 9001 8501

# Set user to root to avoid permission issues when running Streamlit
USER root

# Default command is a shell to allow for manual execution
CMD ["/bin/bash"]