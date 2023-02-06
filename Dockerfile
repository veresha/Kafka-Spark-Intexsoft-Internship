FROM docker.io/bitnami/spark:3.3

ENV SPARK_RPC_AUTHENTICATION_ENABLED no
ENV SPARK_RPC_ENCRYPTION_ENABLED no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED no
ENV SPARK_SSL_ENABLED no

USER root

RUN apt-get update \
    && apt-get -y install netcat gcc libpq-dev zip curl  \
    && apt-get clean

RUN mkdir /usr/checkpoints
RUN chmod -R 777 /usr/checkpoints

WORKDIR /usr/src

COPY install_packages.sh packages.txt ./
RUN ./install_packages.sh

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY entrypoint-spark.sh .
RUN chmod +x entrypoint-spark.sh

COPY src .

USER 1001