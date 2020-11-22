import subprocess
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, desc
from pyspark.sql.window import Window
from util.applogger import getAppLogger
from util.appconfigreader import AppConfigReader

def clearCopyFilesToWorkingDir():
    try:
        logger.info("Copy the old data to temp for merging")
        #Check the working directory and create it if needed
        hdfsCmdResult = subprocess.run(['hdfs', 'dfs', '-test', '-d', workingDir+'/'+table],)
        if hdfsCmdResult.returncode != 0:
            logger.info(f"Working directory {workingDir}/{table} is missing hence creating it first")
            hdfsCmdResult = subprocess.run(['hdfs', 'dfs', '-mkdir', workingDir+'/'+table], check=True)

        #Clear old files from working directory
        logger.info(f"Clear the old data from {workingDir}/{table}")
        hdfsCmdResult = subprocess.run(['hdfs', 'dfs', '-rm', '-r', workingDir+'/'+table+'/*.'+fileFormat], check=True)
        #Copy pervious day base data to working directory
        logger.info(f"Copy the previous day data from {targetDir}/{table} to working directory")
        hdfsCmdResult = subprocess.run(['hdfs', 'dfs', '-cp', targetDir+'/'+table+'/*.'+fileFormat, workingDir+'/'+table], check=True)
        logger.info(f"Files are copied to working directory {workingDir}/{table}")
    except Exception as error:
        logger.exception(f"Moving previous day base data to working directory has failes - {error}")
        raise SystemExit(1)

if __name__ == '__main__':
    try:
        logger = getAppLogger(__name__)
        argParser = argparse.ArgumentParser("Merge and Transform properties file")
        argParser.add_argument("cfgFile", help="CFG file with merge and transform details")
        args = argParser.parse_args()
        if args.cfgFile:
            logger.info(f"Script has been triggered for {args.cfgFile}")
        else:
            logger.error("Script needs the merge and transform details in a properties file")
            raise SystemExit(1)
        #Read config file
        logger.info("Read configuration data")
        appConfigReader = AppConfigReader(args.cfgFile)
        if 'APP' in appConfigReader.config:
            appCfg = appConfigReader.config['APP']
            targetDir = appCfg['TARGET_DIR']
            workingDir = appCfg['WORKING_DIR']
            landingDir = appCfg['LANDING_DIR']
            fileFormat = appCfg['FILE_FORMAT']
            noBuckets = appCfg['NO_BUCKETS']
            destDB = appCfg['DEST_DB']
            bucketByColumn = appCfg['BUCKETBY_COLUMN']
            partitionByColumn = appCfg['PARTITONBY_COLUMN']
            lmtColumn = appCfg['LAST_MODIFIED_TIME_COLUMN']
            fileMode = appCfg['FILE_MODE']
            table = appCfg['TABLE']
        else:
            logger.error("Some configuration are missed")
            raise SystemExit(1)

        #Copy the previous day data to working directory
        clearCopyFilesToWorkingDir()

        #Merge the changes
        logger.info("Create a spark session")
        spark = SparkSession.builder.appName("IncrementalLoadMerge").getOrCreate()
        if spark:
            #load previous day data
            logger.info("Load the previous data")
            baseData = spark.read.format(fileFormat).load(workingDir+'/'+table+'/*.'+fileFormat)
            #load today's changes
            logger.info("Load the latest changes")
            latestChanges = spark.read.format(fileFormat).load(landingDir+'/'+table+'/today/*.'+fileFormat)
            #Combine the datasets
            logger.info("Merge two datasets which will have duplicates for the records modified")
            dataMerge = baseData.union(latestChanges)

            #Filter the old data and save the files with latest changes
            logger.info("Filter the old data and have the latest changes for the records modified")
            dataMerge.select("*", rank().over(Window.partitionBy(partitionByColumn).orderBy(desc(lmtColumn))).alias("latestRecord"))\
                .where("latestRecord == 1").drop("latestRecord").repartition(1).write.option('path',targetDir+'/'+table).mode(fileMode)\
                .bucketBy(noBuckets,bucketByColumn).saveAsTable(destDB+'.'+table)
            logger.info(f"Latest changes merged with the base data and it is available in {targetDir}/{table}")
    except Exception as error:
        logger.exception(f"Failed with error- {error}")
    else:
        logger.info("Latest changes are merged successfully")
    finally:
        spark.stop()
