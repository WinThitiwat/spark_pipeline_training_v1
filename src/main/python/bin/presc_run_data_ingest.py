import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def load_files(spark, file_dir, file_format, header, infer_schema):
    try:
        logger.info("The load_file() function is started...")
        if file_format == "parquet":
            df = spark. \
                    read. \
                    format(file_format). \
                    load(file_dir)
        elif file_format == "csv":
            df = spark. \
                    read. \
                    format(file_format). \
                    options(header=header). \
                    options(inferSchema=infer_schema). \
                    load(file_dir)
            
    except Exception as exp:
        logger.error("Error in the load_files() function. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The input file {file_dir} is successfully loaded to the dataframe. The load_files() is now closed()...")

    return df