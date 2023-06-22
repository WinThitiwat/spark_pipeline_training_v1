#########################################################
# Developed By: Thitiwat Watanajaturaporn               #
# Developed Date: June, 05, 2023                        #
# Script Name:                                          #
# Purpose: Master Script to run the entire project E2E  #
#########################################################

PROJ_FOLDER="/home/${USER}/projects/spark_pipeline_training/src/main/python"

### PART 1
### Call the copy_to_hdfs wrapper to copy the input vendor files from local to HDFS
printf "\n Calling copy_files_local_to_hdfs.ksh at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
${PROJ_FOLDER}/bin/ksh/copy_files_local_to_hdfs.ksh
printf "Executing copy_files_local_to_hdfs.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"

### Call the below wrapper to delete HDFS paths
printf "Calling delete_hdfs_output_paths.ksh at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
${PROJ_FOLDER}/bin/ksh/delete_hdfs_output_paths.ksh
printf "Executing delete_hdfs_output_paths.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"

### Call below Spark Job to extract Fact and City Files
printf "Calling run_presc_pipeline.py at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
spark3-submit --master yarn --num-executors 28 --jars ${PROJ_FOLDER}/lib/postgresql-42.3.5.jar run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"


### PART 2
### Call the below script to copy files from HDFS to Local
printf "Calling copy_files_hdfs_to_local.ksh at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
${PROJ_FOLDER}/bin/ksh/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"

### Call the below script to copy files from Local to S3
printf "Calling copy_files_to_s3.ksh at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
${PROJ_FOLDER}/bin/ksh/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"

### Call the below script to copy files from Local to S3
printf "Calling copy_files_to_azure.ksh at `date +"%d/%m/%Y_%H:%M:%S"`...\n"
${PROJ_FOLDER}/bin/ksh/copy_files_to_azure.ksh
printf "Executing copy_files_to_azure.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"`!!! \n\n"

### Part 3
## Called from Part 1 run_presc_pipeline.py


