import sys,os
path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(1,path)
from src.dependencies.pyspark_class import SparkClass
from transform_fact.fact_internet_sales import FactInternetSales
from src.dependencies.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from src.resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/fact_conf/fact_internet_sales.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


path=conf_file["anather_path"]["fact_reseller_path"]
if os.path.exists(path):
    fact_reseller_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("FactResellerSales")
    fact_reseller_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["product_path"]
if os.path.exists(path):
    prod_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimProduct")
    prod_df=spark_class.extract(jdbc_params=jdbc_values)
    print("the product df ",prod_df)



path=conf_file["anather_path"]["date_path"]
if os.path.exists(path):
    date_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimDate")
    date_df=spark_class.extract(jdbc_params=jdbc_values)


path=conf_file["anather_path"]["employee_path"]
if os.path.exists(path):
    employee_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimEmployee")
    employee_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["promotion_path"]
if os.path.exists(path):
    promotion_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimPromotion")
    promotion_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["currency_path"]
if os.path.exists(path):
    currency_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimCurrency")
    currency_df=spark_class.extract(jdbc_params=jdbc_values)


path=conf_file["anather_path"]["sales_teritory_path"]
if os.path.exists(path):
    sales_territory_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimSalesTerritory")
    sales_territory_df=spark_class.extract(jdbc_params=jdbc_values)



path=conf_file["anather_path"]["dim_reseller_path"]
if os.path.exists(path):
    dim_reseller_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimReseller")
    dim_reseller_df=spark_class.extract(jdbc_params=jdbc_values)


path=conf_file["destination_path"]
if os.path.exists(path):
    response_table_df=spark_class.extract(file_format="parquet",file_path=path,inferSchema=True)
else:
    jdbc_values = get_jdbc_config("FactSurveyResponse")
    response_table_df=spark_class.extract(jdbc_params=jdbc_values)

# Create a DimReseller object

emp_obj = FactInternetSales(spark=spark,
                        logger=logger,
                            )

path=conf_file["destination_path"]
# Perform data transformationa
df = emp_obj.transform( fact_reseller_df=fact_reseller_df,
                        product_df=prod_df,
                        date_df=date_df,
                        employee_df=employee_df,
                        promotion_df=promotion_df,
                        currency_df=currency_df,
                        sales_territory_df=sales_territory_df,
                        dim_reseller_df=dim_reseller_df)

# Write the transformed data to a parquet file
spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")