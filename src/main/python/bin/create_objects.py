from pyspark.sql import SparkSession

import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() is started. The '{envn}' environment is used.")
        if envn == "TEST":
            master = "local"
        else:
            master = "yarn"

        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
        
    except NameError as exp:
        logger.error("NameError in the get_spark_object() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the get_spark_object() method. Please check the stack trace. " + str(exp), exc_info=True)
        
    else:
        logger.info("Spark object is created successfully...")
    
    return spark