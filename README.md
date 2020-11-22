# ETLJob
Data ingestion for HDFS from RDBMS and merging the latest changes.

This ETL job is reponsible for ingesting data for HDFS from RDBMS. 
Sqoop tool is used for data ingestion and Spark is used for merging the daily changes with the base data.
Sqoop import job will be created for each incremetal import and this is a one-time activity.
Once the job is created, it will be executed for the base data preparation for the very first time (inital run) and later on it can be executed for bringing the latest changes to HDFS.
Merge and Transform will handle merging the recent changes with the base data.
For further quick processing, the data is organized in a HIVE external table bucketed by the primary column of the table.

###### Tools used: Sqoop, HIVE and Pyspark

Sqoop Job Creation:
----------------------
#### CMD: ${SCRIPT_PATH}/sqoop_job_creation.sh ${CONFIG_DIR}/sqoop_import_job_creation.cfg

###### Parameter-1  - Config file which is mandatory

List of parameters required in the config file:
1) LOG_PATH - Location of the log file
2) JDBC_URL - JDBC URL of Sqoop metastore
3) USER - User name to connect sqoop metastore
4) PASSWORD_FILE - Password file used to connect sqoop metastore. Should be hidden for security purpose.
5) TARGET_HOME_DIR - Home path in HDFS for the data importation. A sub-directory will be created under it for each table.
6) NO_MAPPER - No of mappers to execute during the RDBMS direct export 
7) MODE - Data importation mode
8) FILE_FORMAT - File format for the data importation which should be matched with one of the formats allowed in sqoop import
9) JOB_NAME - Name of the sqoop import job
10) TABLE - Name of the table to import data
11) LAST_UPDATE_COLUMN - Last modified timestamp column in the table which will be used for incremental loading


Sqoop Job Execution:
---------------------
#### CMD: ${SCRIPT_PATH}/sqoop_incr_import_run.sh [job_name] [initial_run_flag]

###### Parameter-1 - Sqoop job name which is mandatory
###### Parameter-2 - No/YES, Flag to indiate first or incremental run. This is optional, default is incremental (NO)

#### Initial Run: ${SCRIPT_PATH}/sqoop_incr_import_run.sh [job_name] YES
#### Incremental Run: ${SCRIPT_PATH}/sqoop_incr_import_run.sh [job_name]

List of parameters required in the config file:
1) LOG_PATH - Location of the log file
2) JDBC_URL - JDBC URL of Sqoop metastore
3) USER - User name to connect sqoop metastore
4) PASSWORD_FILE - Password file used to connect sqoop metastore. Should be hidden for security purpose.
5) LANDING_HOME - Home path in HDFS for the data importation. Data will be imported to the respective sub-directory under this.

Merge and Transform:
---------------------
#### CMD: spark-submit --master [host/yarn] --py-files util.zip --files mergetransform.properties merge_incremental_load.py ${CONFIG_DIR}/mergetransform.properties

###### Parameter-1 - Config file which is mandatory
Config file should have 2 sections LOGS and APP

List of configurations required under LOG section.
1) LOG_PATH - Location of the log file
2) LOG_LEVEL - Level of logging info
3) LOG_FILE - Name of the log file
4) ERR_FILE - Name of the error file

List of configurations required under APP section.
1) TARGET_DIR - HDFS location of the merged data
2) WORKING_DIR - HDFS working location for temp files
3) LANDING_DIR - HDFS source location for the latest data
4) FILE_FORMAT - File format used to store the data
5) NO_BUCKETS - Number of buckets to create on the merged data
6) DEST_DB - Name of the HIVE database
7) BUCKETBY_COLUMN - Name of the column used for bucketing
8) PARTITONBY_COLUMN - Name of the column used for data partition
9) LAST_MODIFIED_TIME_COLUMN - Name of the column in the table holds the last modified timestamp
10) FILE_MODE - Mode to use for saving the files
11) TABLE - Name of the HIVE external table
