FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root

RUN set -ex; \
    apt-get update && \
    apt-get install -y --no-install-recommends rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER spark
