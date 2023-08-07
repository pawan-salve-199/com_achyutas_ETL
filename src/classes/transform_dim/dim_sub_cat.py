from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimSubCat(SparkClass):
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.jdbc_values=kwargs.get("jdbc_params")
        self.bridge_df  = kwargs.get("bridge_table")
        self.dataframe= kwargs.get("dataframe")
    
    def transform(self):
            """
            Generate product subcategory data and combine it with the existing data.
            Parameters:
                jdbc_values (dict): JDBC connection parameters for reading data from a database.
                bridge_table (pyspark.sql.DataFrame): DataFrame containing bridge data.

            Returns:
                pyspark.sql.DataFrame: Combined DataFrame containing product subcategory data.
            """
            try:
                # Log info message at the start of the function
                self.logger.info("Starting product_sub_cat function.")

                jdbcDF = self.dataframe
                windowSpec = Window.orderBy(lit("A"))
                new_data = prod_sub_cat_data  # Assuming prod_sub_cat_data is defined elsewhere

                new_df = self.spark.createDataFrame(new_data, ['EnglishProductSubcategoryName',
                                                            'SpanishProductSubcategoryName',
                                                            'FrenchProductSubcategoryName'])

                maxid_1 = jdbcDF.select(max(col('ProductSubcategoryKey'))).collect()[0][0]
                maxid_2 = jdbcDF.select(max(col('ProductSubcategoryAlternateKey'))).collect()[0][0]

                new_df = new_df.withColumn("row_number_1", lit(maxid_1) + row_number().over(windowSpec)) \
                            .withColumn("row_number_2", lit(maxid_2) + row_number().over(windowSpec))

                new_df = new_df.withColumn('ProductSubcategoryKey', col("row_number_1")) \
                            .withColumn('ProductSubcategoryAlternateKey', col("row_number_2")) \
                            .drop("row_number_1").drop("row_number_2")

                new_df = new_df.withColumn('ProductCategoryKey', lit(None).cast('integer'))

                new_df = new_df.select('ProductSubcategoryKey', 'ProductSubcategoryAlternateKey',
                                    'EnglishProductSubcategoryName', 'SpanishProductSubcategoryName',
                                    'FrenchProductSubcategoryName', 'ProductCategoryKey')

                bridge = self.bridge_df
                bridge = bridge.withColumnRenamed('EnglishProductSubcategoryName', 'BridgeEnglishProductSubcategoryName') \
                            .withColumnRenamed('SpanishProductSubcategoryName', 'BridgeSpanishProductSubcategoryName') \
                            .withColumnRenamed('FrenchProductSubcategoryName', 'BridgeFrenchProductSubcategoryName')

                final_df = new_df.join(bridge, new_df.EnglishProductSubcategoryName == bridge.BridgeEnglishProductSubcategoryName, 'inner')

                f_df = final_df.select('ProductSubcategoryKey', 'ProductSubcategoryAlternateKey',
                                    'EnglishProductSubcategoryName', 'SpanishProductSubcategoryName',
                                    'FrenchProductSubcategoryName', 'id')

                comb_df = jdbcDF.unionAll(f_df)

                # Log info message at the end of the function
                self.logger.info("product_sub_cat function completed successfully.")
                return comb_df
            except Exception as e:
                # Log the error message
                self.logger.error("Error occurred in product_sub_cat: {}".format(str(e)))
                raise  # Re-raise the exception to handle it further up the call stack



