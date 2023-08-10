from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from src.resources.utils import *
from datetime import datetime

class FactCallCenter:
    def __init__(self, **kwargs):
        """
        Initialize FactCallCenter object.

        Parameters:
            logger: Logger object for logging information.
            spark: SparkSession object.
            dataframe: DataFrame containing the input data.

        Returns:
            None
        """
        self.logger = kwargs.get("logger")
        self.spark = kwargs.get("spark")
        self.dataframe = kwargs.get("dataframe")

    def transform(self):
        """
        Perform the transformation on the DataFrame.

        Returns:
            pyspark.sql.DataFrame: Transformed DataFrame.
        """
        try:
            shift_values = [("AM",), ("PM1",), ("PM2",), ("Midnight",)]
            shift_df = self.spark.createDataFrame(shift_values, ["shift2"])

            df = self.dataframe
            today_date = datetime.now().strftime('%Y%m%d')
            current_shift_data = [Row(DateKey=today_date)]
            current_date_df = self.spark.createDataFrame(current_shift_data, ["DateKey"])

            shift_values2 = ['AM', 'PM1', 'PM2', 'Midnight']
            new_df = current_date_df.withColumn('DateKey', to_date(col('DateKey').cast("string"), "yyyyMMdd")) \
                .withColumn('Date', date_format(col('DateKey'), 'yyyy-MM-dd')) \
                .withColumn('Shift', array(*[lit(x) for x in shift_values2])) \
                .withColumn('Shift2', explode(col('Shift'))) \
                .withColumn('WageType', when(date_format("Date", 'EEE') == 'Mon', 'weekday')
                                        .when(date_format("Date", 'EEE') == 'Tue', 'weekday')
                                        .when(date_format("Date", 'EEE') == 'Wed', 'weekday')
                                        .when(date_format("Date", 'EEE') == 'Thu', 'weekday')
                                        .when(date_format("Date", 'EEE') == 'Fri', 'weekday')
                                        .when(date_format("Date", 'EEE').isin('Sat', 'Sun'), 'holiday')
                                        .otherwise('unknown'))
            min_value = 1
            max_value = 30
            max_calls = 400
            min_calls = 100
            max_auto = 200
            min_auto = 50
            max_orders = 560
            min_orders = 50
            max_grade = 0.60
            min_grade = 0.01
            issue_list = ['0', '1', '2', '3', '4', '5']
            cols_revision = list(map(lambda col_name: lit(col_name), issue_list))

            def sub(arr):
                return random.choice(arr)
            
            issue_udf = udf(sub, StringType())
            new_df = new_df.withColumn("LevelOneOperators", (rand() * (max_value - min_value) + min_value).cast("integer")) \
            .withColumn("LevelTwoOperators", (rand() * (max_value - min_value) + min_value).cast("integer")) \
            .withColumn("TotalOperators", (col('LevelOneOperators') + col('LevelTwoOperators')).cast("integer")) \
            .withColumn("Calls", (rand() * (max_calls - min_calls) + min_calls).cast("integer")) \
            .withColumn('AutomaticResponses', (rand() * (max_auto - min_auto) + min_auto).cast("integer")) \
            .withColumn('Orders', (rand() * (max_orders - min_orders) + min_orders).cast("integer")) \
            .withColumn('IssueRaised', round(rand() * 5).cast("int")) \
            .withColumn("AverageTimePerIssue", (rand() * (max_calls - min_calls) + min_calls).cast("integer")) \
            .withColumn("serviceGrade", (rand() * (max_grade - min_grade) + min_grade).cast("Float")).cache()

            df2 = self.dataframe
            maxid = df2.select(max(col('FactCallCenterID'))).collect()[0][0]
            windowSpec2 = Window.orderBy(lit("A"))
            new_df = new_df.withColumn("row_number", lit(maxid) + row_number().over(windowSpec2)) \
            .withColumn("FactCallCenterID", col("row_number")).drop("row_number") \
            .withColumn('DateKey', date_format(col('DateKey'), 'yyyyMMdd'))
            new_df = new_df.select('FactCallCenterID', 'DateKey', 'WageType', 'shift2', 'LevelOneOperators', 'LevelTwoOperators',
            'TotalOperators',
            'Calls', 'AutomaticResponses', 'Orders', 'IssueRaised', 'AverageTimePerIssue', 'ServiceGrade',
            'Date')
            return new_df
        except Exception as e:
            self.logger.error(f"An error occurred during the transformation: {e}")
            raise e
