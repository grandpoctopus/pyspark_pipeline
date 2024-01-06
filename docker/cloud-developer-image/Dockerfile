FROM datamechanics/spark:3.1.2-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm17
USER root
COPY /. /opt/pyspark-pipeline
RUN cd /opt/pyspark-pipeline \
    && apt-get update && apt-get install vim gcc libkrb5-dev krb5-multidev curl apt-transport-https ca-certificates maven zip -y \
    && curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list \
    && apt-get update \
    && apt-get install -y kubectl \
    && conda create -n pyspark-pipeline pip python==3.8.13 \
    && source activate pyspark-pipeline \
    && conda install -c conda-forge poetry==1.1.14 -y \
    && poetry install

RUN pip3 --no-cache-dir install --upgrade awscli
