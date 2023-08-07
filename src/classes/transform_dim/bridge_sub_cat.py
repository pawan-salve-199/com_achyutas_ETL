from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *


def bridge_sub_cat_product(logger,spark):
    """
    Generate bridge data for subcategories and products.
    Returns:
        pyspark.sql.DataFrame: DataFrame containing bridge data.
    """
    try:
        # Log info message for bridge data generation
        logger.info("Generating bridge data for subcategories and products.")

        # Replace this sample bridge data with your actual data generation logic
        bridge_data = [
            (1, 'Category1', 'Subcategory1', 'Subcategoría1', 'Sous-catégorie1'),
            (2, 'Category2', 'Subcategory2', 'Subcategoría2', 'Sous-catégorie2'),
            (3, 'Category3', 'Subcategory3', 'Subcategoría3', 'Sous-catégorie3'),
        ]

        schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('productcategory', StringType(), False),
            StructField('EnglishProductSubcategoryName', StringType(), False),
            StructField('SpanishProductSubcategoryName', StringType(), False),
            StructField('FrenchProductSubcategoryName', StringType(), False)
        ])

        bridge_df = spark.createDataFrame(bridge_data, schema)

        # Log info message for successful bridge data generation
        logger.info("Bridge data generated successfully.")
        return bridge_df
    except Exception as e:
        # Log the error message
        logger.error("Error occurred in bridge_sub_cat_product: {}".format(str(e)))
        raise  # Re-raise the exception to handle it further up the call stack


        