import sys,os
path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *

# Get the project directory
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Read configuration from JSON file
conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/constant_table.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()          # Start the Spark session


dim_account_path=conf_file["destination_paths"]["dim_account_path"]
if os.path.exists(dim_account_path):
    dim_account=spark_class.extract(file_format="parquet",
                                    file_path=path,
                                    inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimAccount")
    dim_account=spark_class.extract(jdbc_params=jdbc_values)



dim_organization_path=conf_file["destination_paths"]["dim_organization"]
if os.path.exists(dim_organization_path):
    dim_organization=spark_class.extract(file_format="parquet",
                                         file_path=path,
                                         inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimOrganization")
    dim_organization=spark_class.extract(jdbc_params=jdbc_values)



dim_product_path=conf_file["destination_paths"]["dim_product"]
if os.path.exists(dim_product_path):
    dim_product=spark_class.extract(file_format="parquet",
                                    file_path=path,
                                    inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimProduct")
    dim_product=spark_class.extract(jdbc_params=jdbc_values)



dim_sales_reason_path=conf_file["destination_paths"]["dim_sales_reason"]
if os.path.exists(dim_sales_reason_path):
    dim_sales_reason=spark_class.extract(file_format="parquet",
                                         file_path=path,
                                         inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimSalesReason")
    dim_sales_reason=spark_class.extract(jdbc_params=jdbc_values)



dim_scenario_path=conf_file["destination_paths"]["dim_scenario"]
if os.path.exists(dim_scenario_path):
    dim_scenario=spark_class.extract(file_format="parquet",
                                     file_path=path,
                                     inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimScenario")
    dim_scenario=spark_class.extract(jdbc_params=jdbc_values)



dim_department_group_path=conf_file["destination_paths"]["dim_department_group"]
if os.path.exists(dim_department_group_path):
    dim_department_group=spark_class.extract(file_format="parquet",
                                             file_path=path,
                                             inferSchema=True)
else:
    jdbc_values = get_jdbc_config("DimDepartmentGroup")
    dim_department_group=spark_class.extract(jdbc_params=jdbc_values)

spark_class.write_data(dim_account,
                        file_path=dim_account_path,
                        file_format="parquet",
                        mode="append")

spark_class.write_data(dim_organization,
                        file_path=dim_organization_path,
                        file_format="parquet",
                        mode="append")

spark_class.write_data(dim_product, 
                        file_path=dim_product_path,
                        file_format="parquet",
                        mode="append")

spark_class.write_data(dim_sales_reason,
                        file_path=dim_sales_reason_path,
                        file_format="parquet",
                        mode="append")

spark_class.write_data(dim_scenario,
                       file_path=dim_scenario_path,
                        file_format="parquet", 
                        mode="append")

spark_class.write_data(dim_department_group,
                        file_path=dim_department_group_path,
                        file_format="parquet",
                        mode="append")

