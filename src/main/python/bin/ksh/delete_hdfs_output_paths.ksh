#########################################################
# Developed By: Thitiwat Watanajaturaporn               #
# Developed Date: May, 23, 2023                         #
# Script Name:                                          #
# Purpose: Delete HDFS Output paths so that Spark       #
#          extraction will be smooth                    #
#########################################################

# Declare a variable to hold the Unix script name
JOBNAME="delete_hdfs_output_paths.ksh"

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

CITY_PATH=PrescPipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ] then # 0 means the paths are already present in the HDFS
    echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
    # recursive and force delete the CITY_PATH
    hdfs dfs -rm -r -f $CITY_PATH
    echo "The HDFS output directory $CITY_PATH is successfully deleted before extraction."
fi


FACT_PATH=PrescPipeline/output/presc
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ] then # 0 means the paths are already present in the HDFS
    echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
    # recursive and force delete the FACT_PATH
    hdfs dfs -rm -r -f $FACT_PATH
    echo "The HDFS output directory $FACT_PATH is successfully deleted before extraction."
fi

HIVE_CITY_PATH=/user/hive/warehouse/prescpipeline.db/df_city_final
hdfs dfs -test -d $HIVE_CITY_PATH
status=$?
if [ $status == 0 ] then # 0 means the paths are already present in the HDFS
    echo "The HDFS output directory $HIVE_CITY_PATH is available. Proceed to delete."
    # recursive and force delete the HIVE_CITY_PATH
    hdfs dfs -rm -r -f $HIVE_CITY_PATH
    echo "The HDFS output directory $HIVE_CITY_PATH is successfully deleted before extraction."
fi

HIVE_FACT_PATH=/user/hive/warehouse/prescpipeline.db/df_fact_final
hdfs dfs -test -d $HIVE_FACT_PATH
status=$?
if [ $status == 0 ] then # 0 means the paths are already present in the HDFS
    echo "The HDFS output directory $HIVE_FACT_PATH is available. Proceed to delete."
    # recursive and force delete the HIVE_FACT_PATH
    hdfs dfs -rm -r -f $HIVE_FACT_PATH
    echo "The HDFS output directory $HIVE_FACT_PATH is successfully deleted before extraction."
fi

echo "${JOBNAME} is successfully completed...: $(date)"

} > ${LOGFILE} 2>&1 # End of program and end of log.
