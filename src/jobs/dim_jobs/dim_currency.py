import sys,os
from src.dependencies.pyspark_class import SparkClass
from src.transform_dim.dim_currency import DimCurrency
from src.dependencies.logging_class import LoggingConfig
import logging,sys,os
from src.configs.jdbc_conf.config import get_jdbc_config
from src.resources.utils import *


# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_currency.json")

# Configure the logger
log_obj = LoggingConfig(config=conf_file["log_config"])
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)


spark = spark_class.start_spark()

path=conf_file["destination_path"]
if os.path.exists(path):
    dim_currecny=spark_class.extract(file_format="parquet",file_path=path)
else:
    jdbc_values = get_jdbc_config("DimCurrency")
    dim_currecny=spark_class.extract(jdbc_params=jdbc_values)



# Create a DimReseller object
dim_currency_obj = DimCurrency(spark=spark, logger=logger, dataframe=dim_currecny)

# Perform data transformationa
df = dim_currency_obj.transform()

# # Write the transformed data to a parquet file
spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")