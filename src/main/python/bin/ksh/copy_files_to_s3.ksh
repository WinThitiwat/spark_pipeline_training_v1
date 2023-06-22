#########################################################
# Developed By: Thitiwat Watanajaturaporn               #
# Developed Date: May, 25, 2023                         #
# Script Name:                                          #
# Purpose: Copy input vendor files from local to S3     #
#########################################################

# Declare a variable to hold the Unix script name
JOBNAME="copy_files_to_s3.ksh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d_%H:%M:%S')

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

### Push City and Fact files to S3 bucket
for file in ${LOCAL_CITY_DIR}/*.*
do
    aws s3 --profile myprofile cp ${file} "s3://win-prescpipeline/dimension_city/${bucket_subdir_name}/"
    echo "City file: ${file} is pushed to S3"
done

for file in ${LOCAL_FACT_DIR}/*.*
do
    aws s3 --profile myprofile cp ${file} "s3://win-prescpipeline/presc/${bucket_subdir_name}/"
    echo "Presc file: ${file} is pushed to S3"
done

echo "${JOBNAME} is successfully completed...: $(date)"

} > ${LOGFILE} 2>&1 # End of program and end of log.
