FROM docker.io/bitnami/spark:3.3 as base

ENV SPARK_RPC_AUTHENTICATION_ENABLED no
ENV SPARK_RPC_ENCRYPTION_ENABLED no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED no
ENV SPARK_SSL_ENABLED no

USER root

RUN apt-get update \
    && apt-get -y install netcat gcc libpq-dev zip curl  \
    && apt-get clean

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /usr/src

COPY clickhouse-jdbc-0.3.2-patch11-all.jar .

FROM base AS app

RUN mkdir /usr/checkpoints
RUN chmod -R 777 /usr/checkpoints

COPY install_packages.sh packages.txt ./
RUN ./install_packages.sh

COPY entrypoint-spark-minio.sh .
RUN chmod +x entrypoint-spark-minio.sh

COPY entrypoint-clickhouse.sh .

COPY src/minio_reader_app .

RUN mkdir /usr/src/data
RUN chmod -R 777 /usr/src/data


