# pig_env/Dockerfile.pig
#
# Este Dockerfile construye la imagen para el servicio Apache Pig.
# Se encarga de instalar las dependencias necesarias, incluyendo Java,
# Hadoop y Pig, así como el conector de MongoDB para Hadoop/Pig (mongo-hadoop).

# 1. Imagen base: Ubuntu 20.04 (Focal Fossa)
FROM ubuntu:20.04

# Añade un ARG para "romper" el caché de la capa COPY si su valor cambia.
ARG BUILD_DATE="2025-06-07-FINAL-FIX-V5" # ¡CAMBIA ESTO PARA FORZAR REBUILD!

# 2. Instalar dependencias del sistema en una sola capa:
#    - openjdk-11-jdk-headless: Java Development Kit (JDK) versión 11, esencial para Hadoop, Pig y Gradle.
#    - wget, unzip: Para descargar y extraer.
#    - python3, python3-pip: Para los scripts auxiliares
#    - iputils-ping: Para la prueba de conectividad 'ping'
# Se eliminan las líneas que intentaban cambiar el mirror de apt, confiando en los defaults de Ubuntu.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless wget unzip python3 python3-pip iputils-ping && \
    # Limpiar caché de apt
    rm -rf /var/lib/apt/lists/*

# 3. Configurar JAVA_HOME:
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# 4. Instalar Hadoop:
ENV HADOOP_VERSION 3.2.1
RUN wget --inet4-only https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz
ENV HADOOP_HOME /opt/hadoop-${HADOOP_VERSION}
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 5. Instalar Apache Pig:
ENV PIG_VERSION 0.17.0
RUN wget --inet4-only https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz -O /tmp/pig.tar.gz && \
    tar -xzf /tmp/pig.tar.gz -C /opt/ && \
    rm /tmp/pig.tar.gz
ENV PIG_HOME /opt/pig-${PIG_VERSION}
ENV PATH $PATH:$PIG_HOME/bin

# 6. Copiar la configuración de Hadoop (core-site.xml):
COPY core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml

# 7. Descargar e Instalar el Conector MongoDB Hadoop y el Driver Java (¡precompilados!)
# Añadimos json-simple para JsonStorage
ENV MONGO_HADOOP_VERSION 2.0.2

# --- INICIO DEPURACIÓN Y POTENCIAL SOLUCIÓN DE PROXY ---
# Prueba de conectividad de red antes de wget.
# Si el ping falla, el build fallará aquí con exit code 1.
RUN echo "DEBUG: Probando conectividad desde la capa de descarga de JARs..." && \
    ping -c 3 google.com || (echo "ERROR: Ping a google.com falló. Problema de red/DNS en el contenedor." && exit 1) && \
    echo "DEBUG: Conectividad OK. Procediendo con descargas."

# Si estás detrás de un proxy, descomenta y configura estas líneas.
# ENV HTTP_PROXY="http://your.proxy.server:port"
# ENV HTTPS_PROXY="http://your.proxy.server:port"

RUN mkdir -p ${PIG_HOME}/lib && \
    wget --inet4-only https://repo.maven.apache.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-core/${MONGO_HADOOP_VERSION}/mongo-hadoop-core-${MONGO_HADOOP_VERSION}.jar -O ${PIG_HOME}/lib/mongo-hadoop-core-${MONGO_HADOOP_VERSION}.jar && \
    wget --inet4-only https://repo.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-pig/${MONGO_HADOOP_VERSION}/mongo-hadoop-pig-${MONGO_HADOOP_VERSION}.jar -O ${PIG_HOME}/lib/mongo-hadoop-pig-${MONGO_HADOOP_VERSION}.jar && \
    wget --inet4-only https://oss.sonatype.org/content/repositories/releases/org/mongodb/mongo-java-driver/3.12.11/mongo-java-driver-3.12.11.jar -O ${PIG_HOME}/lib/mongo-java-driver-3.12.11.jar && \
    wget --inet4-only https://repo1.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar -O ${PIG_HOME}/lib/json-simple-1.1.1.jar

# Eliminar variables de proxy si se configuraron (por seguridad)
# RUN unset HTTP_PROXY HTTPS_PROXY
# --- FIN DEPURACIÓN Y POTENCIAL SOLUCIÓN DE PROXY ---

# 8. Configurar PIG_CLASSPATH: (se configura en run_pipeline.sh)

# 9. Instalar dependencias Python para los scripts auxiliares
RUN pip3 install --no-cache-dir pymongo redis

# 10. Establecer el directorio de trabajo predeterminado dentro del contenedor:
WORKDIR /pig_scripts

RUN echo "BUILD_DATE: $BUILD_DATE"
