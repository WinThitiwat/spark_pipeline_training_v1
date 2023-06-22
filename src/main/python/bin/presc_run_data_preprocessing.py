import logging
import logging.config
from os import path
from pyspark.sql.functions import upper, lit, col, regexp_extract, concat_ws, count, isnan, when, avg, round, coalesce
from pyspark.sql.window import Window

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

def perform_dim_data_clean(df1):
    """
    Clean df_city dataframe
    1. Select only required columns
    2. Convert city, state and county fields to upper case
    """
    try:
        logger.info("perform_dim_data_clean() is started...")
        df_city_sel = df1.select(
                        upper(df1.city).alias("city"),
                        df1.state_id,
                        upper(df1.state_name).alias("state_name"),
                        upper(df1.county_name).alias("county_name"),
                        df1.population,
                        df1.zips,
                    )
    except Exception as exp:
        logger.error("Error in the perform_dim_data_clean() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_dim_data_clean() is successfully completed.")
    return df_city_sel

def perform_fct_data_clean(df1):
    """
    Clean df_fact dataframe
    1. Select only required columns
    2. Rename the columns
    3. Add a country field 'USA'
    4. Clean years_of_exp field
    5. Convert the years_of_exp datatype from string to number
    6. Combine first name and last name
    7.1 Check and clean all teh Null/Nan values
    7.2 Handle Null/Nan values
    8. Impute TRX_CNT where it is Null as avg of TRX_CNT for the prescriber
    """
    try:
        logger.info("perform_fct_data_clean() is started...")
        # 1. Select only required columns
        # 2. Rename the columns
        df_fact_sel = df1.select(
                        df1.npi.alias("presc_id"),
                        df1.nppes_provider_last_org_name.alias("presc_lname"),
                        df1.nppes_provider_first_name.alias("presc_fname"),
                        df1.nppes_provider_city.alias("presc_city"),
                        df1.nppes_provider_state.alias("presc_state"),
                        df1.specialty_description.alias("presc_spclt"),
                        df1.years_of_exp,
                        df1.drug_name,
                        df1.total_claim_count.alias("trx_cnt"),
                        df1.total_day_supply,
                        df1.total_drug_cost
                    )
        
        # 3. Add a country field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))

        # 4. Clean years_of_exp field
        pattern = r"\d+"
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp",
                                             regexp_extract(col("years_of_exp"), pattern, idx)
                                             )
        # 5. Convert the years_of_exp datatype from string to number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))
        
        # 6. Combine first name and last name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname") # remove unused column

        # 7.1 Check and clean all teh Null/Nan values
        # df_fact_sel.select([ count( when(isnan(cl) | col(cl).isNull(), cl )).alias(cl) 
        #                     for cl in df_fact_sel.columns ]).show()

        # 7.2 Handle Null/Nan values
        # delete the records where the PRESC_ID is null
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

        # delete the records where the DRUG_NAME is null
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

        # 8. Impute TRX_CNT where it is Null as avg of TRX_CNT for the prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn(
                            "trx_cnt",
                            coalesce("trx_cnt", round(avg("trx_cnt").over(spec)))
                        )

        df_fact_sel = df_fact_sel.withColumn("trx_cnt", col("trx_cnt").cast("int"))
        
        df_fact_sel.select([ count( when(isnan(cl) | col(cl).isNull(), cl )).alias(cl) 
                            for cl in df_fact_sel.columns ]).show()
        
    except Exception as exp:
        logger.error("Error in the perform_fct_data_clean() method. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_fct_data_clean() is successfully completed.")
    return df_fact_sel

