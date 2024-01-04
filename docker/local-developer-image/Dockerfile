FROM datamechanics/spark:3.1.2-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm17
USER root
ARG GIT_USER
ARG GIT_PRIVATE_TOKEN
ARG GIT_URL
ARG PYSPARK_PIPELINE_BRANCH=master
RUN apt-get update && apt-get install vim gcc git curl apt-transport-https ca-certificates zip -y
RUN conda create -n spark-query pip python==3.8.13 \
    && source activate spark-query \
    && cd /etc \
    && git config --global http.sslverify false \
    && git clone https://${GITLAB_USER}:${GITLAB_PRIVATE_TOKEN}@${GIT_URL} \
    && cd spark-query \
    && git checkout -b ${PYSPARK_PIPELINE_BRANCH} || echo "building from master branch" \
    && git pull origin ${PYSPARK_PIPELINE_BRANCH} \
    && git remote set-url origin ${GIT_URL} \
    && conda install -c conda-forge poetry==1.1.14 -y \
    && poetry install

RUN pip3 --no-cache-dir install --upgrade awscli
