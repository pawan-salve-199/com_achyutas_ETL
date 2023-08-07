from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimCurrency():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.dataframe=kwargs.get("dataframe")

    def transform(self):
        """
        Generate Currency data and combine it with the existing data.
        
        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing currency data.
        """
        try:
            self.logger.info("Generating Currency data and combining it with the existing data.")
            jdbcDF=self.dataframe
            max_id = jdbcDF.select(max(col('CurrencyKey'))).collect()[0][0]
            windowSpec2 = Window.orderBy(lit("A"))
            new_data = [("IRR", "Iran"), ("IQD", "Iraq")]
            new_df = self.spark.createDataFrame(new_data, ["CurrencyAlternateKey", "CurrencyName"])
            new_df = new_df.withColumn("row_number", lit(max_id) + row_number().over(windowSpec2))
            new_df = new_df.withColumn("CurrencyKey", col("row_number")).drop("row_number")
            new_df = new_df.select("CurrencyKey", "CurrencyAlternateKey", "CurrencyName")
            combine_df = jdbcDF.unionAll(new_df)
            self.logger.info("Currency data generated Successfully and combined with the existing data.")
            return combine_df
        except Exception as e:
            self.logger.error(f"Error Occurred in dim_currency : {e}.")
