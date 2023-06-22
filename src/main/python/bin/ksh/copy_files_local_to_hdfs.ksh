#########################################################
# Developed By: Thitiwat Watanajaturaporn               #
# Developed Date: May, 23, 2023                         #
# Script Name:                                          #
# Purpose: Copy input vendor files from local to HDFS   #
#########################################################

# Declare a variable to hold the Unix script name
JOBNAME="copy_files_local_to_hdfs.ksh"

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

LOCAL_STAGING_PATH="/home/${USER}/projects/spark_pipeline_training/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=PrescPipeline/staging
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copy the City and Fact files from Local to HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/

echo "${JOBNAME} is successfully completed...: $(date)"

} > ${LOGFILE} 2&>1 # End of program and end of log.