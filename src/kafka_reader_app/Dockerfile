FROM docker.io/bitnami/spark:3.3 as Base

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

FROM base AS app

RUN mkdir /usr/checkpoints
RUN chmod -R 777 /usr/checkpoints #check

COPY install_packages.sh packages.txt ./
RUN ./install_packages.sh

COPY src/kafka_reader_app .

COPY src/spark_instance.py .

RUN mkdir /usr/src/data
RUN chmod -R 777 /usr/src/data
