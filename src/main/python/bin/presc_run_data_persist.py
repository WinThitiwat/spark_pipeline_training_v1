import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def data_persist(spark, df, dfName, partitionBy, mode):
    try:
        logger.info("Data Persist - data_persist() is started for saving dataframe " + dfName + " into Hive table...")

        # Add a static column with the current date

        OUTPUT_TBL = "prescpipeline"
        OUTPUT_LOC = "hdfs://localhost:9000/user/hive/warehouse/prescpipeline.db"

        df = df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(f""" CREATE DATABASE IF NOT EXISTS {OUTPUT_TBL} LOCATION '{OUTPUT_LOC}' """)
        spark.sql(f""" USE {OUTPUT_TBL} """)

        df.write.saveAsTable(dfName, partitionBy=partitionBy, mode=mode)
    except Exception as exp:
        logger.error("Error in the data_persis() method. Please check the stack trace: " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Data Persist - data_persist() is completed for saving dataframe " + dfName + " into Hive table...")