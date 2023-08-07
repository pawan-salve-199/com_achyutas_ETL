from classes.pyspark_class import SparkClass
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimDate(SparkClass):
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.jdbc_values=kwargs.get("jdbc_params")
        self.dataframe=kwargs.get("dataframe")

    def transform(self):
        try:
            # Log info message for date data generation
            self.logger.info("Generating date data and combining it with the existing data.")

            jdbcDF = self.dataframe
            t_schema = StructType([StructField("sr_no", IntegerType(), False)])
            t_data = [(i,) for i in range(1, 32)]
            t = self.spark.createDataFrame(t_data, schema=t_schema)
            t1 = t.withColumn("a", expr(f"date_add(current_date(), sr_no - 7)"))

            if 'a' not in jdbcDF.columns:
                jdbcDF = jdbcDF.withColumn('a', lit(None).cast('Date'))

            if 'sr_no' not in jdbcDF.columns:
                jdbcDF = jdbcDF.withColumn('sr_no', lit(None).cast('integer'))

            if 'DateKey' not in t1.columns:
                t1 = t1.withColumn('DateKey', lit(None).cast("integer"))

            if 'FullDateAlternateKey' not in t1.columns:
                t1 = t1.withColumn('FullDateAlternateKey', lit(None).cast('Date'))

            if 'DayNumberOfWeek' not in t1.columns:
                t1 = t1.withColumn('DayNumberOfWeek', lit(None).cast('integer'))

            if 'EnglishDayNameOfWeek' not in t1.columns:
                t1 = t1.withColumn('EnglishDayNameOfWeek', lit(None).cast('string'))

            if 'SpanishDayNameOfWeek' not in t1.columns:
                t1 = t1.withColumn('SpanishDayNameOfWeek', lit(None).cast('string'))

            if 'FrenchDayNameOfWeek' not in t1.columns:
                t1 = t1.withColumn('FrenchDayNameOfWeek', lit(None).cast('string'))

            if 'DayNumberOfMonth' not in t1.columns:
                t1 = t1.withColumn('DayNumberOfMonth', lit(None).cast('integer'))

            if 'DayNumberOfYear' not in t1.columns:
                t1 = t1.withColumn('DayNumberOfYear', lit(None).cast('integer'))

            if 'WeekNumberOfYear' not in t1.columns:
                t1 = t1.withColumn('WeekNumberOfYear', lit(None).cast('integer'))

            if 'EnglishMonthName' not in t1.columns:
                t1 = t1.withColumn('EnglishMonthName', lit(None).cast('string'))

            if 'SpanishMonthName' not in t1.columns:
                t1 = t1.withColumn('SpanishMonthName', lit(None).cast('string'))

            if 'FrenchMonthName' not in t1.columns:
                t1 = t1.withColumn('FrenchMonthName', lit(None).cast('string'))

            if 'MonthNumberOfYear' not in t1.columns:
                t1 = t1.withColumn('MonthNumberOfYear', lit(None).cast('integer'))

            if 'CalendarQuarter' not in t1.columns:
                t1 = t1.withColumn('CalendarQuarter', lit(None).cast('integer'))

            if 'CalendarYear' not in t1.columns:
                t1 = t1.withColumn('CalendarYear', lit(None).cast('integer'))

            if 'CalendarSemester' not in t1.columns:
                t1 = t1.withColumn('CalendarSemester', lit(None).cast('integer'))

            if 'FiscalQuarter' not in t1.columns:
                t1 = t1.withColumn('FiscalQuarter', lit(None).cast('integer'))

            if 'FiscalYear' not in t1.columns:
                t1 = t1.withColumn('FiscalYear', lit(None).cast('integer'))

            if 'FiscalSemester' not in t1.columns:
                t1 = t1.withColumn('FiscalSemester', lit(None).cast('integer'))

            t1 = t1.select('DateKey', 'FullDateAlternateKey', 'DayNumberOfWeek', 'EnglishDayNameOfWeek',
                        'SpanishDayNameOfWeek',
                        'FrenchDayNameOfWeek',
                        'DayNumberOfMonth', 'DayNumberOfYear', 'WeekNumberOfYear', 'EnglishMonthName',
                        'SpanishMonthName',
                        'FrenchMonthName', 'MonthNumberOfYear',
                        'CalendarQuarter', 'CalendarYear', 'CalendarSemester', 'FiscalQuarter', 'FiscalYear',
                        'FiscalSemester',
                        'sr_no', 'a')

            date_df = t1.withColumn("DateKey", date_format(col("a"), "yyyyMMdd").cast('int')) \
                .withColumn("FullDateAlternateKey", col("a")) \
                .withColumn("DayNumberOfWeek", dayofweek(col("a"))) \
                .withColumn("EnglishDayNameOfWeek", date_format(col("a"), "EEEE")) \
                .withColumn("SpanishDayNameOfWeek", date_format(col("a"), "EEEE")) \
                .withColumn("FrenchDayNameOfWeek", date_format(col("a"), "EEEE")) \
                .withColumn("DayNumberOfMonth", date_format(expr("a"), "d")) \
                .withColumn("DayNumberOfYear", date_format(expr("a"), "D")) \
                .withColumn("WeekNumberOfYear", weekofyear(col('a'))) \
                .withColumn("EnglishMonthName", date_format(expr("a"), "MMMM")) \
                .withColumn("SpanishMonthName", date_format(expr("a"), "MMMM")) \
                .withColumn("FrenchMonthName", date_format(expr("a"), "MMMM")) \
                .withColumn("MonthNumberOfYear", date_format(expr("a"), "M")) \
                .withColumn("CalendarQuarter", date_format(expr("a"), "q")) \
                .withColumn("CalendarYear", year(col('a'))) \
                .withColumn("CalendarSemester", date_format(expr("a"), "q")) \
                .withColumn("FiscalQuarter", date_format(expr("date_add(a, 3)"), "q")) \
                .withColumn("FiscalYear", year(col('a'))) \
                .withColumn("FiscalSemester", date_format(expr("date_add(a, 3)"), "q"))

            date_df = date_df.withColumn("DateKey", date_df["DateKey"].cast(IntegerType()))

            date_df = date_df.withColumn("SpanishDayNameOfWeek",
                                        when(dayofweek(col("a")) == 1, lit("lunes"))
                                        .when(dayofweek(col("a")) == 2, lit("martes"))
                                        .when(dayofweek(col("a")) == 3, lit("miércoles"))
                                        .when(dayofweek(col("a")) == 4, lit("jueves"))
                                        .when(dayofweek(col("a")) == 5, lit("viernes"))
                                        .when(dayofweek(col("a")) == 6, lit("sábado"))
                                        .when(dayofweek(col("a")) == 7, lit("domingo"))
                                        .otherwise(lit("")))

            date_df = date_df.withColumn("FrenchDayNameOfWeek",
                                        when(dayofweek(col("a")) == 1, lit("lundi"))
                                        .when(dayofweek(col("a")) == 2, lit("mardi"))
                                        .when(dayofweek(col("a")) == 3, lit("mercredi"))
                                        .when(dayofweek(col("a")) == 4, lit("jeudi"))
                                        .when(dayofweek(col("a")) == 5, lit("vendredi"))
                                        .when(dayofweek(col("a")) == 6, lit("samedi"))
                                        .when(dayofweek(col("a")) == 7, lit("dimanche"))
                                        .otherwise(lit("")))

            date_df = date_df.withColumn("FrenchMonthName",
                                        when(date_format(col("a"), "M") == 1, lit("janvier"))
                                        .when(date_format(col("a"), "M") == 2, lit("février"))
                                        .when(date_format(col("a"), "M") == 3, lit("mars"))
                                        .when(date_format(col("a"), "M") == 4, lit("avril"))
                                        .when(date_format(col("a"), "M") == 5, lit("mai"))
                                        .when(date_format(col("a"), "M") == 6, lit("juin"))
                                        .when(date_format(col("a"), "M") == 7, lit("juillet"))
                                        .when(date_format(col("a"), "M") == 8, lit("août"))
                                        .when(date_format(col("a"), "M") == 9, lit("septembre"))
                                        .when(date_format(col("a"), "M") == 10, lit("octobre"))
                                        .when(date_format(col("a"), "M") == 11, lit("novembre"))
                                        .when(date_format(col("a"), "M") == 12, lit("décembre"))
                                        .otherwise(lit("")))

            date_df = date_df.withColumn("SpanishMonthName",
                                        when(date_format(col("a"), "M") == 1, lit("enero"))
                                        .when(date_format(col("a"), "M") == 2, lit("febrero"))
                                        .when(date_format(col("a"), "M") == 3, lit("marzo"))
                                        .when(date_format(col("a"), "M") == 4, lit("abril"))
                                        .when(date_format(col("a"), "M") == 5, lit("mayo"))
                                        .when(date_format(col("a"), "M") == 6, lit("junio"))
                                        .when(date_format(col("a"), "M") == 7, lit("julio"))
                                        .when(date_format(col("a"), "M") == 8, lit("agosto"))
                                        .when(date_format(col("a"), "M") == 9, lit("septiembre"))
                                        .when(date_format(col("a"), "M") == 10, lit("octubre"))
                                        .when(date_format(col("a"), "M") == 11, lit("noviembre"))
                                        .when(date_format(col("a"), "M") == 12, lit("diciembre"))
                                        .otherwise(lit("")))

            # Combine jdbcDF and date_df after ensuring column types match
            combined_df = jdbcDF.unionByName(date_df)
            combined_df.drop('sr_no', 'a')
            # Log info message for successful date data generation
            self.logger.info("Date data generated successfully and combined with the existing data.")
            return combined_df
        except Exception as e:
            self.logger.error("Error occurred in dim_date: {}".format(str(e)))
            raise  





