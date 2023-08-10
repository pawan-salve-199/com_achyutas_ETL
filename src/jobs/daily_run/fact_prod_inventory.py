import sys,os
# path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
# sys.path.insert(1,path)
from src.src.dependencies.pyspark_class import SparkClass
from src.transform_fact.fact_prod_inventory import FactProdInventory
from src.src.dependencies.logging_class import LoggingConfig
import logging,sys,os
from src.configs.jdbc_conf.config import get_jdbc_config
from src.src.resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/fact_conf/fact_prod_inventory.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


path=conf_file["destination_path"]
if os.path.exists(path):
    prod_cat_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("FactProductInventory")
    prod_cat_df=spark_class.extract(jdbc_params=jdbc_values)


# Create a DimReseller object

emp_obj = FactProdInventory(spark=spark,
                        logger=logger,
                        dataframe=prod_cat_df
                        )

path=conf_file["destination_path"]
# Perform data transformationa
df = emp_obj.transform()

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")