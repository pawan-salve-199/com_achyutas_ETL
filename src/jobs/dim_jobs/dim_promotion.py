import sys,os
path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(1,path)
from src.dependencies.pyspark_class import SparkClass
from src.transform_dim.dim_promotion import DimPromotion
from src.dependencies.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from src.resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_promotion.json")

# Configure the logger
log_obj = LoggingConfig(config=conf_file["log_config"])
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

# Start the Spark session
spark = spark_class.start_spark()

# Get JDBC configuration for DimReseller
jdbc_values = get_jdbc_config("DimPromotion")

# read Dim date table data from sql 
dim_date_df=spark_class.extract(jdbc_params=jdbc_values)
# Create a DimReseller object
dim_date_obj = DimPromotion(spark=spark, logger=logger, dataframe=dim_date_df)

# Perform data transformationa
df = dim_date_obj.transform()

# Define the destination path for writing the output
destination_path = conf_file["destination_path"]

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=destination_path, file_format="parquet", mode="append")