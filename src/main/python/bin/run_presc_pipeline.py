### Import all the necessary modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import sys
import logging
import logging.config
from os import path

from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_dim_data_clean, perform_fct_data_clean
from presc_run_data_transform import city_report, top_5_prescribers_report
from presc_run_data_persist import data_persist
from subprocess import Popen, PIPE
from helper import get_load_file_config
from presc_run_data_extraction import extract_files


log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)


def main():
    try:
        logging.info("main() is started...")
        ### Get Spark object
        spark = get_spark_object(envn=gav.envn, appName=gav.app_name)
        ## Validate Spark Object
        get_curr_date(spark)

        ### Initiatiate run_presc_data_ingest script
        ## Load the City data file
        (file_dir, file_format, header, infer_schema) = get_load_file_config(target="dimension_city")

        df_city = load_files(
                    spark=spark, 
                    file_dir=file_dir, 
                    file_format=file_format,
                    header=header,
                    infer_schema=infer_schema
                )
        
        ## Validate run_data_ingest script for city dimension dataframe
        df_count(df_city, "df_city")
        df_top10_rec(df_city, "df_city")

        ## Load the Prescriber Fact data file
        (file_dir, file_format, header, infer_schema) = get_load_file_config(target="fact")

        df_fact = load_files(
                    spark=spark, 
                    file_dir=file_dir, 
                    file_format=file_format,
                    header=header,
                    infer_schema=infer_schema
                )  
           
        ## Validate
        df_count(df_fact, "df_fact")
        df_top10_rec(df_fact, "df_fact")

        ### Initiatiate run_presc_data_preprocessing script
        ## Perform data cleansing operation
        df_city_sel = perform_dim_data_clean(df_city)
        df_fact_sel = perform_fct_data_clean(df_fact)
        ## Validate
        df_top10_rec(df_city_sel, "df_city_sel")
        df_print_schema(df_fact_sel, "df_fact_sel")
        df_top10_rec(df_fact_sel, "df_fact_sel")


        ### Initiate presc_run_data_transform script
        ## Develop city report and prescriber report
        df_city_final = city_report(df_city_sel=df_city_sel, df_fact_sel=df_fact_sel)
        df_presc_final = top_5_prescribers_report(df_fact_sel=df_fact_sel)

        # validate for df_city_final & df_presc_final
        df_print_schema(df_city_final, "df_city_final")
        df_count(df_city_final, "df_city_final")
        df_top10_rec(df_city_final, "df_city_final")
        
        df_print_schema(df_presc_final, "df_presc_final")
        df_count(df_presc_final, "df_presc_final")
        df_top10_rec(df_presc_final, "df_presc_final")

        ### Initiate run_data_extraction script
        ## Run on HDFS cluster when PROD
        if gav.envn == gav.PROD_ENV:
            CITY_PATH = gav.output_city
            extract_files(df_city_final, 'json', CITY_PATH, 1, False, 'bzip2')
            
            PRESC_PATH = gav.output_fact
            extract_files(df_city_final, 'orc', PRESC_PATH, 2, False, 'snappy')

        ### Persist Data
        # Persist data at Hive
        data_persist(spark=spark, df = df_city_final, dfName="df_city_final", partitionBy="delivery_date", mode="append")
        data_persist(spark=spark, df = df_presc_final, dfName="df_presc_final", partitionBy="delivery_date", mode="append")

        logging.info("presc_run_pipeline.py is completed.")
    except Exception as exp:
        logging.error("Error occurred in the main() method. Please check stack trace. " + str(exp), exc_info=True)
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    logging.info("run_presc_pipeline is started.")
    main()
    sys.exit(0)