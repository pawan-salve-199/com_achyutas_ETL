import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.dim_geography import DimGeography
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_geography.json")

# Configure the logger
log_obj = LoggingConfig(config=conf_file["log_config"])
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

# Start the Spark session
spark = spark_class.start_spark()

# Get JDBC configuration for DimReseller
jdbc_values = get_jdbc_config("DimGeography")

# read Dim geography table data from sql 
geography_df=spark_class.extract(jdbc_params=jdbc_values)
# Create a DimReseller object
dim_geography_obj = DimGeography(spark=spark, logger=logger, dataframe=geography_df)

# Perform data transformationa
df = dim_geography_obj.transform()

# Define the destination path for writing the output
destination_path = conf_file["destination_path"]

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=destination_path, file_format="parquet", mode="overwrite")