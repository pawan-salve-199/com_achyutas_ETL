from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class FactReseller():
    def __init__(self, **kwargs):
        """
        Initialize the FactResponseTable class.

        Parameters:
            logger (logging.Logger): Logger instance for logging messages.
            spark (pyspark.sql.SparkSession): Spark session instance.
            sales_res_df (pyspark.sql.DataFrame): DataFrame containing sales response data.
            prod_cat_df (pyspark.sql.DataFrame): DataFrame containing product category data.
            prod_sub_cat_df (pyspark.sql.DataFrame): DataFrame containing product subcategory data.
            customer_df (pyspark.sql.DataFrame): DataFrame containing customer data.
        """
        self.logger = kwargs.get("logger")
        self.spark = kwargs.get("spark")
        self.fact_reseller_df=kwargs.get("dataframe")
        
    def transform(self):
        df = self.fact_reseller_df

        max_sales_order_number = df.select(max(col('SalesOrderNumber'))).collect()[0][0]


        current_max_number = int(max_sales_order_number[2:])

        sample_df = df.sample(withReplacement=False, fraction=0.15).cache()

        sample_count = sample_df.count()

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
        .withColumn('ShipDateKey', date_format(col('ShipDateKey'), 'yyyyMMdd'))

        return new_df
