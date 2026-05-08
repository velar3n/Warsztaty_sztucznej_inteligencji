FROM apache/airflow:3.2.1-python3.10

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java