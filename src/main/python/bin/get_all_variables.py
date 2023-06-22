import os

TEST_ENV = "TEST"
PROD_ENV = "PROD"

### Set up environment variables
os.environ["envn"] = TEST_ENV
os.environ["header"] = "True"
os.environ["inferSchema"] = "True"

### Get environment variables
envn = os.environ["envn"]
header = os.environ["header"]
infer_schema = os.environ["inferSchema"]

### Set other variables
app_name = "USA Prescriber Research Report"
current_path = os.getcwd()

if envn == TEST_ENV:
    staging_dim_city = current_path + "/../staging/dimension_city"
    staging_fact = current_path + "/../staging/fact"
    # staging_dim_city = current_path + "/src/main/python/staging/dimension_city"
    # staging_fact = current_path + "/src/main/python/staging/fact"
elif envn == PROD_ENV:
    staging_dim_city = "PrescPipeline/staging/dimension_city"
    staging_fact = "PrescPipeline/staging/fact"

    # file_dir = "PrescPipeline/staging/dimension_city"
    # file_dir = "PrescPipeline/staging/fact"

output_city = "PrescPipeline/output/dimension_city"
output_fact = "PrescPipeline/output/presc"

