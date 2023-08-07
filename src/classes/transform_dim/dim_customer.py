from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimCustomer():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.dataframe=kwargs.get("dataframe")
        self.geography_df=kwargs.get("geography_df")

    def transform(self):
        """
        Generate customer data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.
            geography_data (pyspark.sql.DataFrame): DataFrame containing geography data.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing customer data.
        """
        try:
            self.logger.info("Generating customer data and combining it with the existing data.") 
            column_names=['CustomerKey', 'GeographyKey', 'CustomerAlternateKey', 'Title', 'FirstName', 'MiddleName', 'LastName', 'NameStyle', 'BirthDate', 'MaritalStatus', 'Suffix', 'Gender', 'EmailAddress', 'YearlyIncome', 'TotalChildren', 'NumberChildrenAtHome', 'EnglishEducation', 'SpanishEducation', 'FrenchEducation', 'EnglishOccupation', 'SpanishOccupation', 'FrenchOccupation', 'HouseOwnerFlag', 'NumberCarsOwned', 'AddressLine1', 'AddressLine2', 'Phone', 'DateFirstPurchase', 'CommuteDistance']
            df=self.dataframe
            df2 = self.geography_df

            @udf()
            def GeographyKey_udf():
                return random.choice(GeographyKeys)

            GeographyKeys = [i[0] for i in df2.select("GeographyKey").collect()]

            max_id = df.select(max("CustomerKey")).collect()[0][0]

            win=Window.orderBy(monotonically_increasing_id())
            new_df1 = df.withColumn("dummy", lit("dummy")) \
                .withColumn("CustomerKey", max_id+row_number().over(win)) \
                .withColumn("GeographyKey", GeographyKey_udf()) \
                .withColumn("CustomerAlternateKey", concat(lit('AW000')+col("CustomerKey").astype(StringType()))) \
                .withColumn("FirstName", first_name()) \
                .withColumn("LastName", last_name()) \
                .withColumn("BirthDate", BirthDate()) \
                .withColumn("EmailAddress", mail()) \
                .withColumn("AddressLine1", street_address()) \
                .withColumn("Phone", mobile()) \
                .withColumn("DateFirstPurchase", date_sub(current_date(), 3)).drop("dummy")

            max_id= new_df1.select("CustomerKey").collect()[0][0]
            new_df2 = df.withColumn("dummy", lit("dummy")) \
                .withColumn("CustomerKey", max_id+row_number().over(win)) \
                .withColumn("GeographyKey", GeographyKey_udf()) \
                .withColumn("CustomerAlternateKey", concat(lit('AW000')+col("CustomerKey").astype(StringType()))) \
                .withColumn("FirstName", first_name()) \
                .withColumn("LastName", last_name()) \
                .withColumn("BirthDate", BirthDate()) \
                .withColumn("EmailAddress", mail()) \
                .withColumn("AddressLine1", street_address()) \
                .withColumn("Phone", mobile()) \
                .withColumn("DateFirstPurchase", date_sub(current_date(), 3)).drop("dummy")

            combine_df = df.union(new_df1).union(new_df2)
            self.logger.info("Customer data generated successfully and combined with the existing data.")
            return combine_df
        except Exception as e:
            self.logger.error("Error occurred in dim_customer: {}".format(str(e)))