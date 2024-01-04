FROM datamechanics/spark:3.1.2-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm17
USER root
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN apt-get update && apt-get install gcc libkrb5-dev krb5-multidev git curl apt-transport-https ca-certificates zip -y

WORKDIR /etc/pyspark-pipeline

COPY pyproject.toml poetry.lock /etc/pyspark-pipeline/
COPY src /etc/pyspark-pipeline/src
COPY scripts /etc/pyspark-pipeline/scripts
COPY kubernetes /etc/pyspark-pipeline/kubernetes

RUN curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list \
    && apt-get update \
    && apt-get install -y kubectl \
    && apt-get update \
    && conda create -n pyspark-pipeline pip python==3.8.13 \
    && source activate pyspark-pipeline \
    && pip install . \
    && conda pack \
    && pip3 --no-cache-dir install --upgrade awscli
