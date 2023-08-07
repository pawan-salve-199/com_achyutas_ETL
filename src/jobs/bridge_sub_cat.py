import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.bridge_sub_cat import bridge_sub_cat_product
from classes.logging_class import LoggingConfig
import sys,os
from resources.utils import openJson


if __name__=="__main__":
    # Get the project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Read configuration from JSON file
    conf_file = openJson(filepath=f"{project_dir}/src/configs/dim_conf/bridgetable.json")

    # Configure the logger
    log_obj = LoggingConfig(config=conf_file["log_config"])
    logger = log_obj.configure_logger()

    spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)
    spark = spark_class.start_spark()
    bridge_df=bridge_sub_cat_product(logger,spark)
    spark_class.write_data(df=bridge_df,file_path=conf_file["destination_path"],file_format="parquet",mode="overwrite")


print('harshavardhanreddy peddireddy dataengineer')