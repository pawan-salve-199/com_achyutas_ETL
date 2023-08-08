import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.dim_customer import DimCustomer
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_customer.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


path=conf_file["destination_path"]
if os.path.exists(path):
    print("reading from local")
    dim_customer_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimCustomer")
    dim_customer_df=spark_class.extract(jdbc_params=jdbc_values)
print(f"before transforming the customer data : {dim_customer_df.count()}")


#read the geography data
geography_path=conf_file["anather_path"]["geograhy_path"]

if os.path.exists(geography_path):
    print("geo graphy data is here in loca")
    geo_df=spark_class.extract(file_format="parquet",file_path=geography_path)
else:
    geo_jdbc_values=get_jdbc_config("DimGeography")
    geo_df=spark_class.extract(jdbc_params=geo_jdbc_values)


#Create a DimReseller object
dim_customer_obj = DimCustomer(spark=spark,
                                logger=logger, 
                                dataframe=dim_customer_df,
                                geography_df=geo_df)

# Perform data transformationa
df = dim_customer_obj.transform()
print(f"after  transforming the customer data : {df.count()}")

# # Define the destination path for writing the output
# destination_path = conf_file["destination_path"]

# # Write the transformed data to a parquet file
# spark_class.write_data(df, file_path=destination_path, file_format="parquet", mode="overwrite")