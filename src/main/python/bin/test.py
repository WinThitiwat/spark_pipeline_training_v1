import os
import get_all_variables as gav

import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'util/logging_to_file.conf')
### Load the logging config
logging.config.fileConfig(fname=log_file_path)
logger = logging.getLogger(__name__)

print("Hellow world")
# for file in os.listdir(gav.staging_dim_city):
#     print("File is " + file)
#     file_dir = gav.staging_dim_city + "/" + file
#     print(file_dir)

