#########################################################
# Developed By: Thitiwat Watanajaturaporn               #
# Developed Date: May, 23, 2023                         #
# Script Name:                                          #
# Purpose: Copy input vendor files from HDFS to Local   #
#########################################################

# Declare a variable to hold the Unix script name
JOBNAME="copy_files_hdfs_to_local.ksh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a log file where logs would be generated
LOGFILE="/home/${USER}/projects/spark_pipeline_training/src/main/python/logs/${JOBNAME}_${date}.log"

#############################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
#############################################################################
{  # <--- Start of the log file
echo "${JOBNAME} Started...: $(date)"

LOCAL_OUTPUT_PATH="/home/${USER}/projects/spark_pipeline_training/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

HDFS_OUTPUT_PATH=PrescPipeline/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

### Delete the files at Local path if exists
rm -f ${LOCAL_CITY_DIR}/*
rm -f ${LOCAL_FACT_DIR}/*

### Copy the City and Fact file from HDFS to Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/

echo "${JOBNAME} is successfully completed...: $(date)"

} > ${LOGFILE} 2>&1 # End of program and end of log.
