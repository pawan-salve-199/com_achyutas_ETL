from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimGeography(SparkClass):
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.dataframe=kwargs.get("dataframe")

    def transform(self):     
        """
        Generate geography data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing geography data.
        """
        try:
            self.logger.info("Generating Geography data and combining it with the existing data.")
            jdbcDF=self.dataframe
            maxx_ID = jdbcDF.agg(max(col("GeographyKey"))).collect()[0][0]
            maxx_IP = jdbcDF.agg(max(col("IpAddressLocator"))).collect()[0][0]
            new_data = geography_data   # reading the data from utils 

            data_schema = StructType([
                StructField("SalesTerritoryKey", IntegerType(), False),
                StructField("SalesTerritoryRegion", StringType(), False),
                StructField("StateProvinceNAme", StringType(), False),
                StructField("StateProvinceCode", StringType(), False),
                StructField("CountryRegionCode", StringType(), False),
                StructField("versions", ArrayType(StringType(), False), False),
            ])

            df_new = self.spark.createDataFrame(data=new_data, schema=data_schema)
            df_new = df_new.withColumn("City", explode(col('versions'))).drop('versions')
            df1 = df_new.withColumn("random", rand())

            # Scale the random number to a range between 0 and 700000 (800072 - 100072)
            scaled_range = (800072 - 100072)
            df1 = df1.withColumn("random_scaled", round(df1["random"] * scaled_range, 0))

            # Shift the scaled number to a range between 100072 and 800072
            shifted_range = 100072
            df1 = df1.withColumn("PostalCode", df1["random_scaled"].cast("int") + shifted_range)

            # Drop the intermediate columns if needed
            df1 = df1.drop("random", "random_scaled")

            windowSpec = Window.orderBy(lit("A"))

            new_df = df1.withColumn("row_number", lit(maxx_ID) + row_number().over(windowSpec))
            # ProductCategoryKey
            new_df = new_df.withColumn('GeographyKey', col("row_number")).drop("row_number")

            new_df = new_df.withColumn("EnglishCountryRegionName", lit("India"))\
                                .withColumn("SpanishCountryRegionName",lit("India"))\
                                .withColumn("FrenchCountryRegionName", lit("India"))


            new_df = new_df.select("GeographyKey", "City", "StateProvinceCode", "StateProvinceNAme", "CountryRegionCode",
                                "EnglishCountryRegionName", "SpanishCountryRegionName", "FrenchCountryRegionName",
                                "PostalCode", "SalesTerritoryKey").withColumn("IpAddressLocator", IP())
            combine_df = jdbcDF.unionAll(new_df)
            self.logger.info("Geography data generated Successfully and combined with the existing data.")
            return combine_df
        except Exception as e:
            self.logger.error(f"Error Occurred in dim_geography : {e}.")