from src.dependencies.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from src.resources.utils import *

class DimSalesTerritory():
    def __init__(self, **kwargs):
        """
        Initialize the DimSalesTerritory class.

        Parameters:
            logger (logging.Logger): Logger instance for logging messages.
            spark (pyspark.sql.SparkSession): Spark session instance.
            dataframe (pyspark.sql.DataFrame): DataFrame containing sales territory data.
        """
        self.logger = kwargs.get("logger")
        self.spark = kwargs.get("spark")
        self.dataframe = kwargs.get("dataframe")

    def transform(self):
        """
        Generate sales territory data and combine it with the existing data.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing sales territory data.
        """
        jdbcDF=self.dataframe
        maxid = jdbcDF.select(max(col('SalesTerritoryKey'))).collect()[0][0]
        print(maxid)

        next_id = maxid + 1 if maxid is not None else 1

        print(next_id)

        new_data = [('NorthEastInd', 'India', 'Asia'), ('NorthWestInd', 'India', 'Asia'), ('SouthEastInd', 'India', 'Asia')
                    ,('SouthwestInd', 'India', 'Asia' )]

        new_df = self.spark.createDataFrame(new_data,["SalesTerritoryRegion", "SalesTerritoryCountry", "SalesTerritoryGroup"])

        windowSpec = Window.orderBy(lit("A"))
        new_df = new_df.withColumn("row_number", lit(maxid) + row_number().over(windowSpec))

        # Assign row_number as SalesTerritoryKey and SalesTerritoryAlternateKey
        new_df = new_df.withColumn("SalesTerritoryKey", col("row_number")).withColumn("SalesTerritoryAlternateKey", col("row_number")).drop("row_number")

        new_df = new_df.withColumn("SalesTerritoryImage", lit('null').cast(BinaryType()))

        new_df = new_df.select("SalesTerritoryKey", "SalesTerritoryAlternateKey", "SalesTerritoryRegion","SalesTerritoryCountry","SalesTerritoryGroup", "SalesTerritoryImage")

        combine_df = jdbcDF.unionAll(new_df)

        return combine_df