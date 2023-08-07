import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.dim_sub_cat import DimSubCat
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *


if __name__=="__main__":
    # Get the project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Read configuration from JSON file
    conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_sub_cat.json")

    # Configure the logger
    log_obj = LoggingConfig(config=conf_file["log_config"])
    logger = log_obj.configure_logger()

    # Get JDBC configuration for DimReseller
    jdbc_values = get_jdbc_config("DimProductSubcategory")

    # Create a SparkClass instance
    spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

    # Start the Spark session
    spark = spark_class.start_spark()

    # Extract data from bridge table

    bridge_df = spark_class.extract(file_format="parquet",file_path=r"C:\achyutas_ETL_project\com_achyutas_ETL\src\result_dataset\Bridgesubcatproduct2")
    # Create a DimReseller object

    path=conf_file["destination_path"]
    if os.path.exists(path):
        sub_cat_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
    else:
        print("reaidn from jdbc")
        jdbc_values = get_jdbc_config("DimProductCategory")
        sub_cat_df=spark_class.extract(jdbc_params=jdbc_values)

    dim_sub_cat_obj = DimSubCat(spark=spark,
                                logger=logger,
                                bridge_table=bridge_df,
                                dataframe=sub_cat_df)

    # Perform data transformation
    df = dim_sub_cat_obj.transform()

    # Define the destination path for writing the output
    destination_path = conf_file["destination_path"]

    # Write the transformed data to a parquet file
    spark_class.write_data(df, file_path=destination_path, file_format="parquet", mode="overwrite")














    import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.dim_prod_cat import DimProdCat
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/dim_prod_cat.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


path=conf_file["destination_path"]
if os.path.exists(path):
    prod_cat_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    print("reaidn from jdbc")
    jdbc_values = get_jdbc_config("DimProductCategory")
    prod_cat_df=spark_class.extract(jdbc_params=jdbc_values)

print(f"before transforming the prod_cat_df data : {prod_cat_df.count()}")



#Create a DimReseller object
prod_cat_obj = DimProdCat(spark=spark,
                                logger=logger, 
                                dataframe=prod_cat_df)

# Perform data transformationa
df = prod_cat_obj.transform()
print(f"after transforming the prod_cat_df data : {df.count()}")

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")