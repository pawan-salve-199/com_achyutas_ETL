from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from classes.logging_class import LoggingConfig
from resources.utils import *
import random

class DimReseller(SparkClass):
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.jdbc_values=kwargs.get("jdbc_params")
        self.geography_df  = kwargs.get("geography_df")
    
    def read_data(self):
        try:
            dataframe=super().extract(jdbc_params=self.jdbc_values)
            return dataframe
        except Exception as e:
            self.logger.error(f"Exception while reading data: {str(e)}")

    def transform(self):
        try:
            self.logger.info("Generating reseller data and combining it with the existing data.")
            jdbcDF=self.read_data()

            maxid, maxidalt = jdbcDF.select(max(col('ResellerKey')), max(col('ResellerAlternateKey'))).first()

            current_max_number = int(maxidalt[2:])

            # reading geography df
            df_geography =self.geography_df
            key_lists = df_geography.select(collect_list(col('GeographyKey'))).first()[0]

            # Use list comprehensions for creating lists
            cols = [lit(col_name) for col_name in key_lists]
            cols2 = [lit(col_name) for col_name in types]
            col3 = [lit(col_name) for col_name in order_freq]
            col4 = [lit(col_name) for col_name in order_month]
            col5 = [lit(col_name) for col_name in random_years]
            col6 = [lit(col_name) for col_name in random_last_years]
            col7 = [lit(col_name) for col_name in product_line]
            col8 = [lit(col_name) for col_name in banks]
            col9 = [lit(col_name) for col_name in min_payment]
            col10 = [lit(col_name) for col_name in years]

            windowSpec = Window.orderBy(lit("A"))
            new_df = jdbcDF.withColumn("row_number", lit(maxid) + row_number().over(windowSpec))

            new_df = new_df.withColumn(
                "incremented_id",
                (row_number().over(Window.orderBy(lit(0))) - 1) + current_max_number + 1).withColumn(
                "ResellerAlternateKey", format_string("AW%08d", col("incremented_id")))

            new_df = new_df.withColumn('ResellerKey', col("row_number")).drop('row_number')
            new_df = new_df.withColumn('Phone', mobile_udf())
            new_df = new_df.withColumn('GeographyKey', array_choice_udf(array(*cols)))
            new_df = new_df.withColumn('ResellerType', array_type_udf(array(*cols2)))
            new_df = new_df.withColumn('ResellerName', reseller_names_udf())
            new_df = new_df.withColumn('NumberEmployees', reseller_employees_udf(new_df["ResellerType"]))
            new_df = new_df.withColumn('OrderFrequency', order_udf(array(*col3)))
            new_df = new_df.withColumn('OrderMonth', order_month_udf(array(*col4)))
            new_df = new_df.withColumn('FirstOrderYear', order_year_udf(new_df['OrderMonth']))
            new_df = new_df.withColumn('LastOrderYear', order_last_udf(new_df['OrderMonth']))
            new_df = new_df.withColumn('ProductLine', product_line_udf(array(*col7)))
            new_df = new_df.withColumn('AddressLine1', address_udf()).withColumn('AddressLine2', address_udf())
            new_df = new_df.withColumn('AnnualSales', annual_sales_udf())
            new_df = new_df.withColumn('BankName', banks_udf(array(*col8)))
            new_df = new_df.withColumn('MinPaymentType', min_payment_udf(array(*col9)))
            new_df = new_df.withColumn('MinPaymentAmount', min_amount_udf(new_df['MinPaymentType']))
            new_df = new_df.withColumn('AnnualRevenue', annual_sales_udf())
            new_df = new_df.withColumn('YearOpened', years_udf(array(*col10)))

            new_df = new_df.select('ResellerKey', 'ResellerAlternateKey', 'GeographyKey', 'phone', 'ResellerType', 'ResellerName',
                                'NumberEmployees', 'OrderFrequency', 'OrderMonth', 'FirstOrderYear', 'LastOrderYear',
                                'ProductLine', 'AddressLine1', 'AddressLine2', 'AnnualSales', 'BankName', 'MinPaymentType',
                                'MinPaymentAmount', 'AnnualRevenue', 'YearOpened')
            combine_df = jdbcDF.unionAll(new_df)
            self.logger.info("Reseller Data Generated Succesfully and Combined with Existing data")
            return combine_df
        except Exception as e:
            self.logger.error(f"Error Occured in dim_reseller : {e}")


        