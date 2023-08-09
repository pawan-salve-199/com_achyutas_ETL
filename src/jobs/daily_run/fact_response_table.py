import sys,os
path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_fact.fact_serve_response import FactServeResponse
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/fact_conf/fact_serve_response.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


path=conf_file["anather_path"]["prod_cat_path"]
if os.path.exists(path):
    prod_cat_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimProductCategory")
    prod_cat_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["prod_sub_cat_path"]
if os.path.exists(path):
    prod_sub_cat_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimProductSubcategory")
    prod_sub_cat_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["customer_path"]
if os.path.exists(path):
    customer_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimCustomer")
    customer_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["destination_path"]
if os.path.exists(path):
    response_table_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("FactSurveyResponse")
    response_table_df=spark_class.extract(jdbc_params=jdbc_values)

# Create a DimReseller object

emp_obj = FactServeResponse(spark=spark,
                        logger=logger,
                        sales_res_df=response_table_df,
                        prod_cat_df=prod_cat_df,
                        prod_sub_cat_df=prod_sub_cat_df,
                        customer_df=customer_df,
                        )

path=conf_file["destination_path"]
# Perform data transformationa
df = emp_obj.transform()

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")