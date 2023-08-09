import sys,os
path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(1,path)
from classes.pyspark_class import SparkClass
from classes.transform_dim.dim_prod_cat import DimProdCat
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config
from resources.utils import *



if __name__=="__main__":
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
    prod_cat_df.show()
    print(f'the list is : {prod_cat_df.select("ProductCategoryKey").collect()}')





    #Create a DimReseller object
    prod_cat_obj = DimProdCat(spark=spark,
                                    logger=logger, 
                                    dataframe=prod_cat_df)

    # Perform data transformationa
    df = prod_cat_obj.transform()
    keys=df.select(col("ProductCategoryKey")).collect()[0][0]
    print(keys)
    print(f"after transforming the prod_cat_df data : {df.count()}")

    # Write the transformed data to a parquet file
    spark_class.write_data(df, file_path=path, file_format="parquet", mode="append")