
import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    """
    Print the current_date from Spark SQL.
    """
    try:
        output_df = spark.sql(""" SELECT current_date """)
        logger.info("Validate the Spark object by printing current date - " + str(output_df.collect()))
    except NameError as exp:
        logger.error("NameError in the get_curr_date() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the get_curr_date() method. Please check the stack trace. " + str(exp), exc_info=True)
    else:
        logger.info("Spark object is validated. Spark bbject is ready!!! \n\n")

def df_count(df, df_name):
    """
    Print out number of row counts from the given dataframe.
    """
    try:
        logger.info(f"The dataframe validation by count df_count() is started for dataframe '{df_name}...'")
        df_cnt = df.count()
        logger.info(f"The dataframe count is {df_cnt}.")
    except Exception as exp:
        logger.error("Error in the df_count() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by count df_count() is successfully completed!!! \n\n")

def df_top10_rec(df, df_name):
    """
    Print top 10 records from the given dataframe.
    """
    try:
        logger.info(f"The dataframe validation by top 10 record df_top10_rec() is started for dataframe '{df_name}...'")
        logger.info(f"The dataframe top 10 records are: ")
        df_pandas = df.limit(10).toPandas()
        logger.info("\n \t"+df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the df_count() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by top 10 record df_top10_rec() is successfully completed!!! \n\n")

def df_print_schema(df, df_name):
    """
    Print the schema of the given dataframe. 
    """
    try:
        logger.info(f"The dataframe schema validation for dataframe '{df_name}'...")
        scm = df.schema.fields
        logger.info(f"The dataframe '{df_name}' schema is: ")
        for s in scm:
            logger.info(f"\t{s}")
    except Exception as exp:
        logger.error("Error in the df_print_schema() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframe schema validation is completed!!! \n\n")
