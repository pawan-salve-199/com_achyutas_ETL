from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimPromotion():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.dataframe=kwargs.get("dataframe")

    def transform(self):
        """
        Generate promotion data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing promotion data.
        """
        try:
            # Log info message for promotion data generation
            self.logger.info("Generating promotion data and combining it with the existing data.")

            jdbcDF = self.dataframe
            max_id = jdbcDF.select(max(col('PromotionKey'))).collect()[0][0]
            windowSpec = Window.orderBy(lit("A"))
            new_data = promotion_data  # Assuming promotion_data is defined elsewhere

            new_df = self.spark.createDataFrame(new_data,
                                                ['EnglishPromotionName', 'SpanishPromotionName', 'FrenchPromotionName',
                                                 'DiscountPct',
                                                 'EnglishPromotionType', 'SpanishPromotionType', 'FrenchPromotionType',
                                                 'EnglishPromotionCategory', 'SpanishPromotionCategory',
                                                 'FrenchPromotionCategory'])
            new_df = new_df.withColumn("row_number", lit(max_id) + row_number().over(windowSpec))
            new_df = new_df.withColumn('PromotionKey', col("row_number")).withColumn("PromotionAlternateKey",
                                                                                      col("row_number")).drop(
                "row_number")
            new_df = new_df.withColumn('min', lit(0).cast('integer')).withColumn('max', lit(None).cast('integer'))

            new_df = new_df.withColumn("startDate", current_date()).withColumn("endDate", date_add(col("current_date"), 7))
            new_df = new_df.select('PromotionKey', 'PromotionAlternateKey', 'EnglishPromotionName', 'SpanishPromotionName',
                                   'FrenchPromotionName', 'DiscountPct',
                                   'EnglishPromotionType', 'SpanishPromotionType', 'FrenchPromotionType',
                                   'EnglishPromotionCategory', 'SpanishPromotionCategory', 'FrenchPromotionCategory',
                                   'startDate', 'endDate', 'min', 'max')

            combine_df = jdbcDF.unionAll(new_df)
            combine_df = combine_df.withColumn("startDate",
                                               when(col("PromotionKey") == 18, date_add(current_date(), 12))
                                               .otherwise(col("startDate")))
            combine_df = combine_df.withColumn("endDate",
                                               when(col("PromotionKey") == 18, date_add(current_date(), 18))
                                               .otherwise(col("endDate")))

            # Log info message for successful promotion data generation
            self.logger.info("Promotion data generated successfully and combined with the existing data.")
            return combine_df
        except Exception as e:
            self.logger.error("Error occurred in dim_promotion: {}".format(str(e)))
            raise  # Re-raise the exception to handle it further up the call stack
