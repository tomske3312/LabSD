FROM debian:11-slim

# 1) Instala Java, Python y utilidades
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
      openjdk-11-jdk-headless \
      wget unzip \
      python3 python3-pip \
      curl && \
    rm -rf /var/lib/apt/lists/*

# 2) Instala pymongo, redis y Elasticsearch Python client 8.14.0
RUN pip3 install --no-cache-dir \
      pymongo \
      redis \
      elasticsearch==8.14.0

# 3) Variables de entorno en formato key=value
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_VERSION=3.2.1
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig-${PIG_VERSION}
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin

# 4) Descarga y descomprime Hadoop
RUN wget --quiet \
      https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz

# 5) Descarga y descomprime Pig
RUN wget --quiet \
      https://dlcdn.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz \
      -O /tmp/pig.tar.gz && \
    tar -xzf /tmp/pig.tar.gz -C /opt/ && \
    rm /tmp/pig.tar.gz

# 6) Directorio de trabajo
WORKDIR /scripts_auxiliares
