#!/usr/bin/sh

function fct_log
{
  echo "$(date +'%Y%m%d%H%M%S') : $1" >> ${LOG_FILE}
}

#Source the config file
. ${CONFIG_DIR}/sqoop_import_job_execute.cfg

TODAY=$(date +'%Y%m%d%H%M%S')
LOG_FILE=${LOG_PATH}/$(basename $0 .sh)_${TODAY}.log

JOB_NAME=$1
INITIAL_RUN=${2:-NO}

#Check the argument passed
if [ "${JOB_NAME}" == "" ]
then
  fct_log "Job name is mandatory. Example: $0 \"daily_order_import\""
fi

table=$(sqoop job --show ${JOB_NAME} | grep "db.table" | awk -F"=" '{print $2}' | xargs)
incremental_column=$(sqoop job --show ${JOB_NAME} | grep "incremental.col" | awk -F"=" '{print $2}' | xargs)

#Get the last fetch time for incremental run
if [ "${INITIAL_RUN}" == "NO" ]
then
  last_extraction_time=$(sqoop job --show ${JOB_NAME} | grep incremental.last.value | awk -F"=" '{print $2}' | xargs)
  if [ "${last_extraction_time}" == "" ]
  then
    fct_log "Error - Could not get the last extraction time of the job ${JOB_NAME}"
	  exit 1
  fi
  fct_log "Last extraction date is ${last_extraction_time}"
  count_sql_condition="where ${incremental_column} > '${last_extraction_time}'"
else
  count_sql_condition=""
fi

#Get no of records created/modified after last extraction
modification_db_count=$(sqoop eval --connect ${JDBC_URL} --username ${USER} --password-file ${PASSWORD_FILE} --query "select count(*) from ${table} ${count_sql_condition}" | tail -2 | head -1 | awk -F"|" '{print $2}' | xargs)
fct_log "Number of records to load - ${modification_db_count}"

#Move the existing data to archive
hdfs dfs -mv ${LANDING_HOME}/${table}/today ${LANDING_HOME}/${table}/$(date +'%Y%m%d')

#Run the import job
sqoop job --exec ${JOB_NAME}

if [ $? != 0 ]
then
  fct_log "Error - Job execution has failed. Please check the log file $LOG_FILE for more details"
  exit 1
fi

latest_extraction_time = $(sqoop job --show ${JOB_NAME} | grep incremental.last.value | awk -F"=" '{print $2}' | xargs)
fct_log "Data have been exported until ${latest_extraction_time}"

fct_log "Import script has completed for the job-${JOB_NAME}"
exit 0