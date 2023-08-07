from dependencies.spark import PySparkConnector
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from faker import Faker
import random
from datetime import datetime, timedelta

class FactDataGenerate(PySparkConnector):
    def __init__(self,spark_session):
        self.spark=spark_session

    def fact_reseller(self,jdbc_values):
        df = super().read_data(jdbc_params=jdbc_values)
        print('the count of old dataset : ',df.count())
        max_sales_order_number = df.select(max(col('SalesOrderNumber'))).collect()[0][0]
        print(max_sales_order_number) #SO99999

        current_max_number = int(max_sales_order_number[2:])

        sample_df = df.sample(withReplacement=False, fraction=0.15).cache()

        sample_count = sample_df.count()
        print(sample_count) # 99,921

        SalesOrderLineNumber = [1, 2, 3, 4, 5]


        sample_df2 = sample_df.withColumn("incremented_id", row_number().over(Window.orderBy(lit(0))) + current_max_number + 1) \
        .withColumn("SalesOrderNumber", format_string("SO%05d", col("incremented_id"))) \
        .withColumn('SalesOrderLineNumber', array(*[lit(x) for x in SalesOrderLineNumber])) \
        .withColumn('SalesOrderLineNumber', explode(col('SalesOrderLineNumber'))).cache()

        new_df = sample_df2.withColumn('OrderDateKey', date_format(current_date(), 'yyyyMMdd')) \
        .withColumn('OrderDateKey', to_date(col('OrderDateKey').cast("string"), "yyyyMMdd")) \
        .withColumn('DueDateKey', date_add(col('OrderDateKey'), 15)) \
        .withColumn('ShipDateKey', date_add(col('OrderDateKey'), 10)) \
        .withColumn('OrderDate', date_format(col('OrderDateKey'), 'yyyy-MM-dd')) \
        .withColumn('DueDate', date_format(col('DueDateKey'), 'yyyy-MM-dd')) \
        .withColumn('ShipDate', date_format(col('ShipDateKey'), 'yyyy-MM-dd')) \
        .withColumn('OrderDateKey', date_format(col('OrderDateKey'), 'yyyyMMdd')) \
        .withColumn('DueDateKey', date_format(col('DueDateKey'), 'yyyyMMdd')) \
        .withColumn('ShipDateKey', date_format(col('ShipDateKey'), 'yyyyMMdd')).cache()
        print("AFTER THE TRANSFORMATION THE DATA COUNT IS : ",new_df.count())

        return new_df


        # print("parquet_file_path")
        # now = datetime.now()

        # dt_str = now.strftime("%y-%m-%d")

        # print(dt_str)

        # parquet_file = f"Daily/Fact/Reseller_{dt_str}.parquet"
        # new_df.coalesce(1).write.mode('append').parquet(parquet_file)
