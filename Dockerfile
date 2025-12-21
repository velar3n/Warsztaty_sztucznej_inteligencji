FROM apache/airflow:3.0.1

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant curl && \
    apt-get clean;
    # rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip --no-cache-dir install -r /requirements.txt