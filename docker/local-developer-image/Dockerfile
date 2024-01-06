FROM datamechanics/spark:3.1.2-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm17
USER root
COPY /. /opt/pyspark-pipeline
RUN cd /opt/pyspark-pipeline \
    && conda create -n pyspark-pipeline pip python==3.8.13 \
    && source activate pyspark-pipeline \
    && conda install -c conda-forge poetry==1.1.14 -y \
    && poetry install
