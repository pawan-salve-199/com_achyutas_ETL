from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class FactProdInventory():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.dataframe=kwargs.get("dataframe")
        
    def transform(self):
        jdbcDF = self.dataframe

        sample_df = jdbcDF.sample(withReplacement=False, fraction=0.55)

        max_UnitsIn_Out = 498
        min_UnitsIn_Out = 0

        max_unitcost = 1131.25
        min_unitcost = 0.15


        new_df = sample_df.withColumn('DateKey', date_format(current_date(), 'yyyyMMdd')) \
        .withColumn('Year', col('DateKey').substr(1, 4).cast("int")) \
        .withColumn('Month', col('DateKey').substr(5, 2).cast("int")) \
        .withColumn('Day', col('DateKey').substr(7, 2).cast("int")) \
        .withColumn("DateKey", col("DateKey").cast("int")) \
        .withColumn('MovementDate', to_date(concat_ws("-", col('Year'), col('Month'), col('Day')))) \
        .withColumn('UnitsIn', (rand() * (max_UnitsIn_Out - min_UnitsIn_Out) + min_UnitsIn_Out).cast("integer")) \
        .withColumn('UnitCost', (rand() * (max_UnitsIn_Out - min_UnitsIn_Out) + min_UnitsIn_Out).cast("Float")) \
        .withColumn('UnitsOut', (rand() * (max_UnitsIn_Out - min_UnitsIn_Out) + min_UnitsIn_Out).cast("integer")) \
        .withColumn('UnitsBalance', (col('UnitsOut') - col('UnitsIn')))

        new_df = new_df.drop('Year', 'Month', 'Day')

        return new_df

            




