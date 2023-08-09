from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimProdCat():
    def __init__(self, **kwargs):
        """
        Initialize the DimProdCat class.

        Parameters:
            logger (logging.Logger): Logger instance for logging messages.
            spark (pyspark.sql.SparkSession): Spark session instance.
            dataframe (pyspark.sql.DataFrame): DataFrame containing product category data.
        """
        self.logger = kwargs.get("logger")
        self.spark = kwargs.get("spark")
        self.dataframe = kwargs.get("dataframe")

    def transform(self):
        """
        Generate product category data.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing product category data.
        """
        try:
            self.logger.info("Generating productcategory data and combining it with the existing data.") 
            jdbcDF = self.dataframe
            maxid = jdbcDF.select(max(col('ProductCategoryKey'))).collect()[0][0]
            windowSpec = Window.orderBy(lit("A"))

            new_data = [
                ('Software products', 'productos de software', 'Produits logiciels'),
                ('Hardware products', 'productos de Hardware', 'Produits de quincaillerie'),
                ('Books', 'Libros', 'Livres'),
                ('chemicals', 'productos quimicos', 'produits chimiques'),
                ('Food and Beverage', 'alimentos y bebidas', 'Nourriture et boisson'),
                ('Telecommunication and computing', 'Telecommunicacion y computacion', 'Télécommunication et informatique'),
                ('Aerospace', 'Aeroespacial', 'Aérospatial'),
                ('pharmaceuticals', 'farmaceutica', 'médicaments')
            ]

            new_df = self.spark.createDataFrame(new_data, ['EnglishProductCategoryName', 'SpanishProductCategoryName', 'FrenchProductCategoryName'])
            new_df = new_df.withColumn("row_number", lit(maxid) + row_number().over(windowSpec))
            new_df = new_df.withColumn('ProductCategoryKey', col("row_number")).withColumn("ProductCategoryAlternateKey", col("row_number")).drop("row_number")
            new_df = new_df.select('ProductCategoryKey', 'ProductCategoryAlternateKey', 'EnglishProductCategoryName', 'SpanishProductCategoryName', 'FrenchProductCategoryName')
            combine_df = jdbcDF.unionAll(new_df)
            combine_df.show()

            # Log info message for successful product category data generation
            self.logger.info("Product category data generated successfully.")
            return combine_df
        except Exception as e:
            # Log the error message
            self.logger.error("Error occurred in dim_productcategory: {}".format(str(e)))
            raise  # Re-raise the exception to handle it further up the call stack