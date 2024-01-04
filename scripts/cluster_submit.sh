#!/bin/bash
set -o pipefail
set -e
usage() {
cat <<EOF

Usage: bash $0
    -e - USE_EXISTING_CONDA_ENV if defined will not rebuild the current conda
         environment but use it as-is. Otherwise, the environment will be
         rebuilt.
         (optional)
	-f - JOB_FILE for spark-query job. (required)
	-s - SETTINGS_FILE for spark-query job. (required)
    -a - JOB_ARGS for the spark-query job that are passed in double quotes
         example: "-r t -m f"
    -m - SPARK_MASTER (options: yarn, kubernetes, emr) (optional) default is yarn
    -O - file for override_settings
EOF
}

echo "CONDA_DEFAULT_ENV: $CONDA_DEFAULT_ENV"
echo "PACKAGE_REPO_USER: $PACKAGE_REPO_USER"
echo "PACKAGE_REPO_URL: $PACKAGE_REPO_URL"

if [[ -z ${CONDA_DEFAULT_ENV} || "${CONDA_DEFAULT_ENV}" == "base" ]]
then
    echo "You must run this script in an active, non-base conda environment"
    exit 1
fi

while getopts "ef:s:a:m:p:O:" OPTION;
do
    case $OPTION in
	e) USE_EXISTING_CONDA_ENV="T";;
	f) JOB_FILE=$OPTARG;;
	s) SETTINGS_FILE=$OPTARG;;
    a) JOB_ARGS=$OPTARG;;
    m) SPARK_MASTER=$OPTARG;;
    p) PURGE_ALL_CACHES=$OPTARG;;
    O) OVERRIDE_SETTINGS_FILE=$OPTARG;;
    esac
done

if [[ -z ${JOB_FILE} ]]
then
    echo "Option -f can not be empty"
    usage
    exit 1
fi

if [[ ! -f ${JOB_FILE} ]]
then
    echo "job file ${JOB_FILE} does not exist"
    exit 1
fi

if [[ -z ${SETTINGS_FILE} ]]
then
    echo "Option -s can not be empty"
    usage
    exit 1
fi

if [[ ! -f ${SETTINGS_FILE} ]]
then
    echo "settings file ${SETTINGS_FILE} does not exist"
    exit 1
fi

if [[ ! -z ${OVERRIDE_SETTINGS_FILE} && ! -f ${OVERRIDE_SETTINGS_FILE} ]]
then
    echo "Error: OVERRIDE_SETTINGS_FILE specified file but ${OVERRIDE_SETTINGS_FILE} does not exist"
    exit 1
fi

# using the same file for settings file and base settings file
# will result in the same file but I can't find a way to make
# using a base settings flag trult optional in the
# spark-submit script
if [[ -z ${OVERRIDE_SETTINGS_FILE} ]]
then
    OVERRIDE_SETTINGS_FILE=${SETTINGS_FILE}
fi


if [[ -z "${USE_EXISTING_CONDA_ENV}" ]]
then
    if [[ -z ${PACKAGE_REPO_USER} || -z ${PACKAGE_REPO_PASS} || -z ${PACKAGE_REPO_URL} ]]
    then
        echo "Environment variables must be defined for credentials and URL to reach the PI package repo"
        echo "  * PACKAGE_REPO_USER"
        echo "  * PACKAGE_REPO_PASS"
        echo "  * PACKAGE_REPO_URL"
        exit 1
    fi
    _OLD_CONDA_ENV=$CONDA_DEFAULT_ENV
    eval "$(conda shell.bash hook)"
    conda deactivate
    conda env remove -n $_OLD_CONDA_ENV
    rm -f "${_OLD_CONDA_ENV}.tar.gz"
    eval "$(conda shell.bash hook)"
    echo y | conda create -n $_OLD_CONDA_ENV python=3.7.7
    conda activate $_OLD_CONDA_ENV
    if [[ "${PURGE_ALL_CACHES}" == "T" ]]
    then
        pip cache purge
        python -m cache purge
        rm ~/.cache/pip -rf
        rm poetry.lock
        conda install -y poetry==1.1.14 -c conda-forge
        echo yes | poetry cache clear . --all
    fi
    if [[ -z "${PURGE_ALL_CACHES}" ]]
    then
        conda install -y poetry==1.1.14 -c conda-forge
    fi

    poetry install
    poetry build
    echo y | pip uninstall spark-query
    pip install dist/*.whl
fi

if [[ ! -f "${CONDA_DEFAULT_ENV}.tar.gz" ]]
then
    conda list
    conda pack
fi

# master is set to yarn if not specified
if [[ -z ${SPARK_MASTER} ]]
then
    export SPARK_MASTER="emr"
fi

# set up emr specific configuration
if [ "${SPARK_MASTER}" == "emr" ];
then
    export SPARK_MASTER="yarn"
    export SPARK_HOME=/usr/lib/spark
    export SPARK_CONF_DIR=/etc/spark/conf
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    export YARN_CONF_DIR=/etc/hadoop/conf

    export SPARK_CONF="
    --conf spark.pyspark.python=./pyenv/bin/python
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./pyenv/bin/python"
    --conf spark.driver.memory=20g
    --packages net.snowflake:snowflake-jdbc:3.11.1,net.snowflake:spark-snowflake_2.11:2.5.7-spark_2.4
    "
fi

# set up kubernetes specific configuration

if [[ "${SPARK_MASTER}" == "kubernetes" && -z ${S3_CODE_BUCKET} ]]
then
   echo "'S3_CODE_BUCKET' variable must be set to use 'kubernetes' as SPARK_MASTER"
   exit 1
fi

if [[ "${SPARK_MASTER}" == "kubernetes" && -z ${S3_LOGS_BUCKET} ]]
then
   echo "'S3_LOGS_BUCKET' variable must be set to use 'kubernetes' as SPARK_MASTER"
   exit 1
fi

if [[ -z ${KUBE_ADDRESS} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export KUBE_ADDRESS="k8s://kubernetes.default.svc.cluster.local"
fi

if [[ -z ${KUBE_SPARK_IMAGE} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export KUBE_SPARK_IMAGE="datamechanics/spark:3.1.2-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm17"
fi

if [[ -z ${POD_TEMPLATE_FILE} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export POD_TEMPLATE_FILE=$(dirname $( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P ))/kubernetes/pod_templates/mem.yml
fi

if [[ -z ${DRIVER_POD_TEMPLATE_FILE} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export DRIVER_POD_TEMPLATE_FILE=$(dirname $( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P ))/kubernetes/pod_templates/cpu.yml
fi

if [[ -z ${DRIVER_POD_LABEL} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export DRIVER_POD_LABEL='spark-query'
fi

if [[ -z ${K8S_NAMESPACE} && "${SPARK_MASTER}" == "kubernetes" ]]
then
    export K8S_NAMESPACE='spark-query'
fi

if [ "${SPARK_MASTER}" == "kubernetes" ];
then
    export SPARK_MASTER=${KUBE_ADDRESS}
    export JOB_FILE="file://${JOB_FILE}"
    export SPARK_CONF="
    --conf spark.driver.memory=25g
    --conf spark.driver.memoryOverhead=3g
    --conf spark.eventLog.enabled=true
    --conf spark.eventLog.dir=${S3_LOGS_BUCKET}
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
    --conf spark.kubernetes.authenticate.executor.serviceAccountName=spark
    --conf spark.kubernetes.container.image.pullSecrets=artifactory,dockerhub
    --conf spark.kubernetes.container.image=${KUBE_SPARK_IMAGE}
    --conf spark.kubernetes.driver.node.selector.nodegroup=default
    --conf spark.kubernetes.driver.request.cores=7
    --conf spark.kubernetes.driverEnv.PYSPARK_PYTHON=./pyenv/bin/python
    --conf spark.kubernetes.driverEnv.SNOWFLAKE_USER=${SNOWFLAKE_USER}
    --conf spark.kubernetes.driverEnv.SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
    --conf spark.kubernetes.file.upload.path=${S3_CODE_BUCKET}
    --conf spark.kubernetes.executor.podTemplateFile=${POD_TEMPLATE_FILE}
    --conf spark.kubernetes.driver.podTemplateFile=${DRIVER_POD_TEMPLATE_FILE}
    --conf spark.kubernetes.namespace=${K8S_NAMESPACE}
    --conf spark.eventLog.enabled=true
    --conf spark.driver.maxResultSize=5g
    --conf spark.shuffle.service.enabled=false
    --conf spark.sql.session.timeZone=UTC
    --conf spark.dynamicAllocation.enabled=true
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true
    --conf spark.dynamicAllocation.minExecutors=0
    --conf spark.dynamicAllocation.initialExecutors=0
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=gp3
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/data
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false
    --conf spark.kubernetes.driver.label.${DRIVER_POD_LABEL}=true
    "
fi

# print bucket name returns null if the target_path is not an S3 bucket
# if the target_path is an S3 bucket an S3 committer is used to make S3
# writes more efficient
TARGET_S3_BUCKET=`python -m pyspark_pipeline.print_bucket_name_for_spark_submit -s "${SETTINGS_FILE}" -O "${OVERRIDE_SETTINGS_FILE}"`
if [[ ${TARGET_S3_BUCKET} ]]
then
    S3_CONF="
    --conf spark.hadoop.fs.s3a.bucket.${TARGET_S3_BUCKET}.committer.magic.enabled=true
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    --conf spark.hadoop.fs.s3a.committer.magic.enabled=true
    --conf spark.hadoop.fs.s3a.committer.name=magic
    --conf spark.hadoop.fs.s3a.connection.maximum=500
    --conf spark.hadoop.fs.s3a.threads.max=240
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=10
    --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
    --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
    --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
    --conf spark.hadoop.parquet.enable.summary-metadata=false
    --conf spark.sql.parquet.mergeSchema=false
    --conf spark.sql.parquet.filterPushdown=true
    --conf spark.sql.hive.metastorePartitionPruning=true
    "
    SPARK_CONF="${SPARK_CONF} ${S3_CONF}"
fi

SETTINGS_YAML_CONF=`python -m pyspark_pipeline.print_spark_configs_for_spark_submit -s "${SETTINGS_FILE}" -O "${OVERRIDE_SETTINGS_FILE}"`
SPARK_CONF="${SPARK_CONF} ${SETTINGS_YAML_CONF}"



spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode cluster \
    --archives="${CONDA_DEFAULT_ENV}.tar.gz#pyenv" \
    --conf spark.yarn.appMasterEnv.SNOWFLAKE_USER=${SNOWFLAKE_USER} \
    --conf spark.yarn.appMasterEnv.SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD} \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp -XX:+UseG1GC -XX:ParallelGCThreads=16 -XX:ConcGCThreads=8 -XX:InitiatingHeapOccupancyPercent=35" \
    ${SPARK_CONF} \
    "${JOB_FILE}" -M cluster ${REG_TEST_FLAG} -O "`cat ${OVERRIDE_SETTINGS_FILE}`" -s "`cat ${SETTINGS_FILE}`" ${JOB_ARGS}
