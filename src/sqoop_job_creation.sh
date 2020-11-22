#!/usr/bin/sh

if [ $# != 1 ]
then
  echo "Incorrect number of arguments-$# are passed. Script expects 1 argument which is the config file with path"
  echo "sqoop_job_creation.sh /apps/config/daily_order_loading.cfg"
  exit 1
fi

CONFIG_FILE=$1

if [ ! -s ${CONFIG_FILE} ]
then
  echo "Config file-${CONFIG_FILE} is empty. Please check and add the details required"
  exit 1
fi

#Source the config file
. ${CONFIG_FILE}

TODAY=$(date +'%Y%m%d%H%M%S')
LOG_FILE=${LOG_PATH}/$(basename $0 .sh)_${TODAY}.log

echo "Log file for this execution is ${LOG_FILE}"

if [ "${JOB_NAME}" == "" -o "${TABLE}" == "" -o "${LAST_UPDATE_COLUMN}" == "" -o "${JDBC_URL}" == "" -o "${USER}" == "" -o "${PASSWORD_FILE}" == "" -o "${TARGET_HOME_DIR}" == "" -o "${NO_MAPPER}" == "" -o "${MODE}" == "" -o "${FILE_FORMAT}" == "" -o "${JAR_DIR}" == "" ]
then
  echo "Some values are not defined in the config file which are mandatory for the job creation" | tee -a ${LOG_FILE}
  exit 1
fi

hdfs dfs -test -d ${TARGET_HOME_DIR}
if [ $? != 0 ]
then
  echo "${TARGET_HOME_DIR} is missing in HDFS so going to create it first..." | tee -a ${LOG_FILE}
  hdfs dfs -mkdir ${TARGET_HOME_DIR}
  hdfs dfs -chmod -R 744 ${TARGET_HOME_DIR}
fi

hdfs dfs -test -d ${TARGET_HOME_DIR}/${TABLE}
if [ $? != 0 ]
then
  echo "${TARGET_HOME_DIR}/${TABLE} is missing in HDFS so going to create it first..." | tee -a ${LOG_FILE}
  hdfs dfs -mkdir ${TARGET_HOME_DIR}/${TABLE}
  hdfs dfs -chmod -R 744 ${TARGET_HOME_DIR}/${TABLE}
fi

echo "Create the sqoop import job - ${JOB_NAME}..." | tee -a ${LOG_FILE}
sqoop job --create ${JOB_NAME} -- import --connect ${JDBC_URL} --username ${USER} --password-file ${PASSWORD_FILE} --table ${TABLE} --target-dir ${TARGET_HOME_DIR}/${TABLE}/today --incremental lastmodified --check-column ${LAST_UPDATE_COLUMN} -m ${NO_MAPPER} --${MODE} --${FILE_FORMAT}

if [ $? != 0 ]
then
  echo "Error - Job creation has failed. Please check the log file $LOG_FILE for more details" | tee -a ${LOG_FILE}
  exit 1
fi

echo "Job-${JOB_NAME} has been created" | tee -a ${LOG_FILE}
target_dir=$(sqoop job --show ${JOB_NAME} | grep "hdfs.target.dir" | awk -F"=" '{print $2}')
table=$(sqoop job --show ${JOB_NAME} | grep "db.table" | awk -F"=" '{print $2}')
file_format=$(sqoop job --show ${JOB_NAME} | grep "hdfs.file.format" | awk -F"=" '{print $2}')
incremental_column=$(sqoop job --show ${JOB_NAME} | grep "incremental.col" | awk -F"=" '{print $2}')
echo -e "Here are the job details\n Table-${table}\n  Key Column-${incremental_column}\n Directory-${target_dir}\n File Format-${file_format}" | tee -a ${LOG_FILE}

exit 0