from classes.pyspark_class import SparkClass
from jobs.DimTransform import DataGenerater
from classes.logging_class import LoggingConfig
import logging,sys,os
from configs.jdbc_conf.config import get_jdbc_config

project_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 

log_config={"file_path":f"{project_dir}/src/app_logs/log_file.log",
             "log_level":logging.INFO,
             "log_formatter":"%(asctime)s - %(levelname)s - Line %(lineno)d - %(message)s"}

logger=LoggingConfig(config=log_config)
# path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\app_logs\log_file.log"
logger=logger.configure_logger()
print("the logger object is ",logger)

spark_conf={"spark.driver.extraClassPath":f'{project_dir}/src/resources/mssql-jdbc-12.2.0.jre11.jar'}

conf={"app_name" :"pawan",
      "master" : "local[*]",
       "spark_config" : spark_conf}

spark_con=SparkClass(config=conf,logger_obj=logger)
spark = spark_con.start_spark()
generate=DataGenerater(spark,logger)
# produce_data=FactDataGenerate(spark_session=spark)

# ----------------------------DimSalesTerritory-----------------------


jdbc_values=get_jdbc_config("DimSalesTerritory")
data=spark_con.extract(jdbc_params=jdbc_values)
destination_path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\DimSalesTerritory"
spark_con.write_data(data,file_path=destination_path,file_format="parquet",mode="overwrite")


# #-----------------------------bridge_df---------------------------------

# bridge_df=generate.bridge_sub_cat_product()
# parquet_file = r'C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\Bridgesubcatproduct2'
# spark_con.write_data(df=bridge_df,file_path=parquet_file,file_format="parquet",mode="overwrite")

# #----------------------DimProductSubcategory --------------------

# jdbc_values=get_jdbc_config("DimProductSubcategory")

# df=generate.product_sub_cat(jdbc_values,bridge_table=bridge_df)
# dest_path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\DimProductSubcategory"
# spark_con.write_data(df=df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #--------------------------DimDate------------------------

# jdbc_values=get_jdbc_config("DimDate")
# date_df=generate.dim_date(jdbc_values=jdbc_values)

# dest_path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\DimDate"
# print('writing the data')
# spark_con.write_data(df=date_df.coalesce(2),file_path=dest_path,file_format="parquet",mode="overwrite")

# #-----------------------------dim_promotion-------------------------------

# jdbc_values=get_jdbc_config("DimPromotion")
# promotion_df=generate.dim_date(jdbc_values=jdbc_values)
# promotion_df=promotion_df.coalesce(2)
# dest_path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\DimPromotion"
# spark_con.write_data(df=promotion_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# # --------------------------------geography---------------------------------  
# jdbc_values=get_jdbc_config("DimGeography")
# geography_data=generate.dim_geography(jdbc_values=jdbc_values)
# dest_path=r"C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1\ResultDataset\DimGeography"

# spark_con.write_data(df=geography_data,file_path=dest_path,file_format="parquet",mode="overwrite")

# #-----------------------DimReseller-----------------   
# jdbc_values=get_jdbc_config("DimReseller")
# g_df=spark_con.read_data(file_path="ResultDataset/DimGeography/export (1).csv",file_format="csv",header=True, inferSchema=True)
# reseller_df=generate.dim_reseller(jdbc_values=jdbc_values,geography_data=g_df)

# dest_path="ResultDataset\DimReseller"
# spark_con.write_data(df=reseller_df,file_path=dest_path,file_format="parquet",mode="overwrite")


# #----------------------------Dim_currency-----------------------
# jdbc_values=get_jdbc_config("DimCurrency")
# currency_df=generate.dim_currency(jdbc_values=jdbc_values)
# dest_path="ResultDataset\DimCurrency"
# spark_con.write_data(df=currency_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# # -----------------------------dim_customer------------------------    need geography data

# jdbc_values=get_jdbc_config("DimCustomer")
# dest_path="ResultDataset/DimCustomer"
# geography_data=spark_con.read_data(file_format="csv",file_path="ResultDataset\DimGeography\export (1).csv",header=True, inferSchema=True)
# customer_df=generate.dim_customer(geography_data=geography_data,jdbc_values=jdbc_values)
# spark_con.write_data(df=customer_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #-------------------------------------DimAccount----------------------------------
# jdbc_values=get_jdbc_config("DimAccount")
# dest_path="ResultDataset/DimAccount"
# account_df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=account_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #-------------------------------------DimProduct--------------------------------
# jdbc_values=get_jdbc_config("DimProduct")
# dest_path="ResultDataset/DimProduct"
# product_df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=product_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #------------------------------------DimScenario-----------------------------------
# jdbc_values=get_jdbc_config("DimScenario")
# dest_path="ResultDataset/DimScenario"
# scenario_df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=scenario_df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #-----------------------------------dim_orginazation----------------------
# jdbc_values=get_jdbc_config("DimOrganization")
# dest_path="ResultDataset/DimOrganization"
# organization_df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=organization_df,file_path=dest_path,file_format="parquet",mode="overwrite")
# #----------------------------------------DimSalesReason----------------------
# jdbc_values=get_jdbc_config("DimSalesReason")
# dest_path="ResultDataset/DimSalesReason"
# df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=df,file_path=dest_path,file_format="parquet",mode="overwrite")
# #-------------------------------dim_department_group--------------------------
# jdbc_values=get_jdbc_config("DimDepartmentGroup")
# dest_path="ResultDataset/DimDepartmentGroup"
# df=spark_con.read_data(jdbc_params=jdbc_values)
# spark_con.write_data(df=df,file_path=dest_path,file_format="parquet",mode="overwrite")

# #-----------------------------dim_productcategory-----------------------------
# jdbc_values=get_jdbc_config("DimProductCategory")
# dest_path="ResultDataset/DimProductCategory"
# df=generate.dim_productcategory(jdbc_values=jdbc_values)
# spark_con.write_data(df=df,file_path=dest_path,file_format="avro",mode="overwrite")


# #--------------------------------dim_employee----------------------------------
# jdbc_values=get_jdbc_config("DimEmployee")
# empDF= spark_con.read_data( jdbc_params=jdbc_values,header=True , inferSchema=True)
# datedf = spark_con.read_data(file_path="ResultDataset\DimDate",file_format="parquet",header=True , inferSchema=True)
# salesteritoryDF= spark_con.read_data(file_path="ResultDataset\DimSalesTerritory",file_format="parquet",header=True , inferSchema=True)

# dest_path="ResultDataset/DimEmployee"
# df=generate.dim_employee(jdbc_values=jdbc_values,employee_df=empDF,date_df=datedf,sales_teritory_df=salesteritoryDF)
# spark_con.write_data(df=df,file_path=dest_path,file_format="parquet",mode="overwrite")



# #-------------------------------------fact reseller----------------------------------

# jdbc_values=get_jdbc_config("FactReseller")
# dest_path="ResultDataset/FactReseller"
# reseller_df=produce_data.fact_reseller(jdbc_params=jdbc_values)
# reseller_df.show()
# spark_con.write_data(df=df,file_path=dest_path,file_format="parquet",mode="overwrite")