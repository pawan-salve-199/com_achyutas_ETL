from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from faker import Faker
from src.dependencies.pyspark_class import SparkClass
from src.dependencies.logging_class import LoggingConfig
from src.resources.utils import *
import random,logging

class DataGenerater(SparkClass,LoggingConfig):
    def __init__(self, spark_session,logger_obj):
        """
        Initialize the DataGenerate object.
        Parameters:
            spark_session (pyspark.sql.SparkSession): The Spark session object.
        """
        self.spark = spark_session
        # self.logger=LoggingConfig(logging.INFO).configure_logger()
        self.logger=logger_obj
        fake=Faker()

    def product_sub_cat(self, jdbc_values, bridge_table):
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

                jdbcDF = super().read_data(jdbc_params=jdbc_values)
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

                bridge = bridge_table
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
            

    def bridge_sub_cat_product(self):
        """
        Generate bridge data for subcategories and products.
        Returns:
            pyspark.sql.DataFrame: DataFrame containing bridge data.
        """
        try:
            # Log info message for bridge data generation
            self.logger.info("Generating bridge data for subcategories and products.")

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

            bridge_df = self.spark.createDataFrame(bridge_data, schema)

            # Log info message for successful bridge data generation
            self.logger.info("Bridge data generated successfully.")
            return bridge_df
        except Exception as e:
            # Log the error message
            self.logger.error("Error occurred in bridge_sub_cat_product: {}".format(str(e)))
            raise  # Re-raise the exception to handle it further up the call stack

    def dim_date(self, jdbc_values):
        """
        Generate date data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing date data.
        """
        try:
            # Log info message for date data generation
            self.logger.info("Generating date data and combining it with the existing data.")

            jdbcDF = super().read_data(jdbc_params=jdbc_values, file_format="jdbc")
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

    def dim_promotion(self, jdbc_values):
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

            jdbcDF = super().read_data(jdbc_params=jdbc_values)
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

    def dim_reseller(self,jdbc_values,geography_data):   #defect
        """
        Generate reseller data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.
            geography_data (pyspark.sql.DataFrame): GeoGraphy DataFrame 

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing reseller data.
        """
        try:
            self.logging.info("Generating reseller data and combining it with the existing data.")

            jdbcDF=super().read_data(jdbc_params=jdbc_values)
            types = ['Value Added Reseller', 'Specialty Bike Shop', 'Warehouse']
            order_freq = ['A', 'Q', 'S']
            order_month = [3, 4, 5, 6, 8, 1, 12, 10, None]
            random_years = [2011, 2012, 2008, 2005, 2010, 2013, 2014, 2006]
            random_last_years = [2011, 2012, 2014, 2023]
            product_line = ['Road', 'Mountain', 'Touring']
            banks = ['Primary International', 'United Security', 'Primary Bank & Reserve', 'Guardian Bank', 'HDFC', 'DBS', 'AXIS',
                        'ReserveSecurity']
            min_payment = [1, 2, 3, None]
            years = [1984, 1988, 1984, 1991, 1996, 1998, 1992, 2002, 2004, 2003, 1993, 1999]

            array_choice_udf = udf(lambda arr: random.choice(arr), IntegerType())
            array_type_udf = udf(lambda arr: random.choice(arr), StringType())
            mobile_udf = udf(lambda: fake.basic_phone_number(), StringType())
            reseller_names_udf = udf(lambda: fake.name(), StringType())
            reseller_employees_udf = udf(lambda reseller_type: random.randint(10, 25) if reseller_type in types[1:] else None,IntegerType())
            order_udf = udf(lambda arr: random.choice(arr), StringType())
            order_month_udf = udf(lambda arr: random.choice(arr), IntegerType())
            order_year_udf = udf(lambda order_month: random.choice(random_years) if order_month else None, IntegerType())
            order_last_udf = udf(lambda order_month: random.choice(random_last_years) if order_month else None,IntegerType())
            product_line_udf = udf(lambda arr: random.choice(arr), StringType())
            address_udf = udf(lambda: fake.address(), StringType())
            annual_sales_udf = udf(lambda: random.randint(8000000, 300000000), IntegerType())
            banks_udf = udf(lambda arr: random.choice(arr), StringType())
            min_payment_udf = udf(lambda arr: random.choice(arr), IntegerType())
            min_amount_udf = udf(lambda min_payment: 600 if min_payment == 3 else (300 if min_payment == 2 else None),
                                    IntegerType())
            years_udf = udf(lambda arr: random.choice(arr), IntegerType())
            jdbcDF = super().read_data(jdbc_params=jdbc_values)


            maxid, maxidalt = jdbcDF.select(max(col('ResellerKey')), max(col('ResellerAlternateKey'))).first()

            current_max_number = int(maxidalt[2:])

            # reading geography df
            df_geography =geography_data
            key_lists = df_geography.select(collect_list(col('GeographyKey'))).first()[0]

            # Use list comprehensions for creating lists
            cols = [lit(col_name) for col_name in key_lists]
            cols2 = [lit(col_name) for col_name in types]
            col3 = [lit(col_name) for col_name in order_freq]
            col4 = [lit(col_name) for col_name in order_month]
            col5 = [lit(col_name) for col_name in random_years]
            col6 = [lit(col_name) for col_name in random_last_years]
            col7 = [lit(col_name) for col_name in product_line]
            col8 = [lit(col_name) for col_name in banks]
            col9 = [lit(col_name) for col_name in min_payment]
            col10 = [lit(col_name) for col_name in years]

            windowSpec = Window.orderBy(lit("A"))
            new_df = jdbcDF.withColumn("row_number", lit(maxid) + row_number().over(windowSpec))

            new_df = new_df.withColumn(
                "incremented_id",
                (row_number().over(Window.orderBy(lit(0))) - 1) + current_max_number + 1).withColumn(
                "ResellerAlternateKey", format_string("AW%08d", col("incremented_id")))

            new_df = new_df.withColumn('ResellerKey', col("row_number")).drop('row_number')
            new_df = new_df.withColumn('Phone', mobile_udf())
            new_df = new_df.withColumn('GeographyKey', array_choice_udf(array(*cols)))
            new_df = new_df.withColumn('ResellerType', array_type_udf(array(*cols2)))
            new_df = new_df.withColumn('ResellerName', reseller_names_udf())
            new_df = new_df.withColumn('NumberEmployees', reseller_employees_udf(new_df["ResellerType"]))
            new_df = new_df.withColumn('OrderFrequency', order_udf(array(*col3)))
            new_df = new_df.withColumn('OrderMonth', order_month_udf(array(*col4)))
            new_df = new_df.withColumn('FirstOrderYear', order_year_udf(new_df['OrderMonth']))
            new_df = new_df.withColumn('LastOrderYear', order_last_udf(new_df['OrderMonth']))
            new_df = new_df.withColumn('ProductLine', product_line_udf(array(*col7)))
            new_df = new_df.withColumn('AddressLine1', address_udf()).withColumn('AddressLine2', address_udf())
            new_df = new_df.withColumn('AnnualSales', annual_sales_udf())
            new_df = new_df.withColumn('BankName', banks_udf(array(*col8)))
            new_df = new_df.withColumn('MinPaymentType', min_payment_udf(array(*col9)))
            new_df = new_df.withColumn('MinPaymentAmount', min_amount_udf(new_df['MinPaymentType']))
            new_df = new_df.withColumn('AnnualRevenue', annual_sales_udf())
            new_df = new_df.withColumn('YearOpened', years_udf(array(*col10)))

            new_df = new_df.select('ResellerKey', 'ResellerAlternateKey', 'GeographyKey', 'phone', 'ResellerType', 'ResellerName',
                                'NumberEmployees', 'OrderFrequency', 'OrderMonth', 'FirstOrderYear', 'LastOrderYear',
                                'ProductLine', 'AddressLine1', 'AddressLine2', 'AnnualSales', 'BankName', 'MinPaymentType',
                                'MinPaymentAmount', 'AnnualRevenue', 'YearOpened')
            combine_df = jdbcDF.unionAll(new_df)
            self.logger.info("Reseller Data Generated Succesfully and Combined with Existing data")
            return combine_df
        except Exception as e:
            self.logger.error(f"Error Occured in dim_reseller : {e}")

    def dim_geography(self,jdbc_values):  #defect
        """
        Generate geography data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing geography data.
        """
        try:
            self.logger.info("Generating Geography data and combining it with the existing data.")
            jdbcDF=super().read_data(jdbc_params=jdbc_values)
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

    def dim_currency(self,jdbc_values):
        """
        Generate Currency data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing currency data.
        """
        try:
            self.logger.info("Generating Currency data and combining it with the existing data.")
            jdbcDF=super().read_data(jdbc_params=jdbc_values)
            max_id = jdbcDF.select(max(col('CurrencyKey'))).collect()[0][0]
            windowSpec2 = Window.orderBy(lit("A"))
            new_data = [("IRR", "Iran"), ("IQD", "Iraq")]
            new_df = self.spark.createDataFrame(new_data, ["CurrencyAlternateKey", "CurrencyName"])
            new_df = new_df.withColumn("row_number", lit(max_id) + row_number().over(windowSpec2))
            new_df = new_df.withColumn("CurrencyKey", col("row_number")).drop("row_number")
            new_df = new_df.select("CurrencyKey", "CurrencyAlternateKey", "CurrencyName")
            combine_df = jdbcDF.unionAll(new_df)
            self.logger.info("Currency data generated Successfully and combined with the existing data.")
            return combine_df
        except Exception as e:
            self.logger.error(f"Error Occurred in dim_currency : {e}.")

    def dim_customer(self,jdbc_values,geography_data):
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
            df=super().read_data(jdbc_params=jdbc_values).toDF(*column_names)
            df2 = geography_data

            @udf()
            def GeographyKey_udf():
                return random.choice(GeographyKeys)
            @udf()
            def first_name():
                return fake.first_name()
            @udf()
            def last_name():
                return fake.last_name()
            @udf()
            def mail():
                return fake.free_email()
            @udf()
            def BirthDate():
                return fake.passport_dob().strftime("%Y-%m-%d")
            @udf()
            def mobile():
                return fake.basic_phone_number()
            @udf()
            def street_address():
                return fake.street_address()

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
    

    def dim_productcategory(self,jdbc_values):
        """
        Generate product category data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing product category data.
        """
        try:
            self.logger.info("Generating productcategory data and combining it with the existing data.") 
            jdbcDF = super().read_data(jdbc_params=jdbc_values)
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

    
    def dim_employee(self,jdbc_values,**kargs):
        """
        Generate employee data and combine it with the existing data.
        Parameters:
            jdbc_values (dict): JDBC connection parameters for reading data from a database.
            **kargs: Additional keyword arguments.
                employee_df (pyspark.sql.DataFrame): DataFrame containing employee data.
                date_df (pyspark.sql.DataFrame): DataFrame containing date data.
                sales_teritory_df (pyspark.sql.DataFrame): DataFrame containing sales territory data.
        Returns:
            pyspark.sql.DataFrame: Combined DataFrame containing employee data.
        """
        try:
            self.logger.info("Generating employee data and combining it with the existing data.") 
            empDF = kargs.get("employee_df")
            datedf = kargs.get("date_df")
            salesteritoryDF = kargs.get("sales_teritory_df")
            max_emp_id = empDF.select(max("EmployeeKey")).collect()[0][0]
            dateids = [i[0] for i in datedf.select('FullDateAlternateKey').collect()]
            win = Window.partitionBy("dummy").orderBy("EmployeeKey")
            SalesTerritoryKey = [i[0] for i in salesteritoryDF.select("SalesTerritoryKey").collect()]

            @udf()
            def first_name():
                return fake.first_name()

            @udf()
            def last_name():
                return fake.last_name()

            @udf()
            def mail():
                return fake.free_email()

            @udf()
            def SalesTerritoryKey_udf():
                return random.choice(SalesTerritoryKey)

            @udf()
            def BirthDate():
                return fake.passport_dob().strftime("%Y-%m-%d")

            @udf()
            def mobile():
                return fake.basic_phone_number()

            @udf()
            def hire_date():
                return random.choice(dateids)

            df = (empDF
                .withColumn("dummy", lit("dummy"))
                .withColumn("EmployeeKey", max_emp_id + row_number().over(win))
                .withColumn("FirstName", first_name())
                .withColumn("LastName", last_name())
                .withColumn("EmailAddress", mail())
                .withColumn("SalesTerritoryKey", SalesTerritoryKey_udf())
                .withColumn("HireDate", hire_date())
                .withColumn("BirthDate", BirthDate())
                .withColumn("StartDate", current_date())
                .withColumn("Phone", mobile())
                .withColumn("EmergencyContactPhone", mobile())
                .drop("dummy")
            )
            new_df = empDF.union(df)

            # Log info message for successful employee data generation
            self.logger.info("Employee data generated successfully.")
            return new_df
        except Exception as e:
            # Log the error message
            self.logger.error("Error occurred in dim_employee: {}".format(str(e)))
            raise  # Re-raise the exception to handle it further up the call stack

    


    

