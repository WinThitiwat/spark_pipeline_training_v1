import get_all_variables as gav
import os
from subprocess import Popen, PIPE

def get_load_file_config(target):
    """
    Get the right Spark arguments for different environment and target file
    """
    if (gav.envn == gav.TEST_ENV):
            if target == "dimension_city":
                 target_dir = gav.staging_dim_city
            elif target == "fact":
                 target_dir = gav.staging_fact

            for file in os.listdir(target_dir):
                file_dir = target_dir + "/" + file

                if file.split(".")[1] == "csv":
                    file_format = "csv"
                    header=gav.header
                    infer_schema=gav.infer_schema
                elif file.split(".")[1] == "parquet":
                    file_format = "parquet"
                    header="NA"
                    infer_schema="NA"

    elif (gav.envn == gav.PROD_ENV):
        if target == "dimension_city":
            file_dir = gav.staging_dim_city
        elif target == "fact":
            file_dir = gav.staging_fact

        _process =  Popen(["hdfs", "dfs", "-ls", "-C", file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = _process.communicate()

        if "csv" in out.decode(): # convert str to univode
                file_format = "csv"
                header=gav.header
                infer_schema=gav.infer_schema
        elif "parquet" in out.decode(): 
            file_format = "parquet"
            header="NA"
            infer_schema="NA"
    
    return (file_dir, file_format, header, infer_schema)