import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def extract_files(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info("Extraction - extract_files() is started...")

        df.coalesce(split_no) \
            .write \
            .format(format) \
            .save(filePath, header=headerReq, compression=compressionType)
        
    except Exception as exp:
        logger.error("", exc_info=True)
        raise
    else:
        logger.info("Extraction - extract_files() is successfully completed...")