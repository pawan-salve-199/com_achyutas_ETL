from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from src.resources.utils import *
from datetime import datetime

class FactInternetSales:
    def __init__(self, **kwargs):
        self.logger = kwargs.get("logger")
        self.spark = kwargs.get("spark")

    def transform(self,**kwargs):
        fact_reseller_df = kwargs.get("fact_reseller_df")
        product_df = kwargs.get("product_df")
        Date_df = kwargs.get("date_df")
        emp_df = kwargs.get("employee_df")
        promotion_df = kwargs.get("promotion_df")
        currency_df = kwargs.get("currency_df")
        sales_teritory_df = kwargs.get("sales_territory_df")
        dim_reseller_df = kwargs.get("dim_reseller_df")

        # Extract distinct values from DataFrames

        prod_list = [(i[0], i[1]) for i in product_df.select("ProductKey", "StandardCost").distinct().collect()]

        date_list = [i[0] for i in Date_df.select("DateKey").exceptAll(fact_reseller_df.select("OrderDateKey")).collect()]

        resellerKey_list = [i[0] for i in dim_reseller_df.select("ResellerKey").distinct().collect()]

        emp_list = [i[0] for i in emp_df.select("EmployeeKey").collect()]

        promotion_list = [i[0] for i in promotion_df.select("PromotionKey").collect()]

        currency_list = [i[0] for i in currency_df.select("CurrencyKey").collect()]

        sales_teritory_list = [i[0] for i in sales_teritory_df.select("SalesTerritoryKey").collect()]

        

        # Find the maximum sales order id

        max_sales_order_id = fact_reseller_df.select(max(split("SalesOrderNumber", 'SO')[1]).astype(IntegerType())).collect()[0][0]

        

        # Generate sample data

        l = []

        for i in range(10):

            dates = random.choice(date_list)

            reseller_key = random.choice(resellerKey_list)

            emp_key = random.choice(emp_list)

            promotion_keys = random.choice(promotion_list)

            currency_keys = random.choice(currency_list)

            sales_teritory_keys = random.choice(sales_teritory_list)

            revision = random.choice([1, 2])

            for j in range(1, random.randint(1, 20)):

                l.append([random.choice(prod_list), dates, reseller_key, emp_key, promotion_keys, currency_keys, sales_teritory_keys, "SO"+str(max_sales_order_id + i), j, revision, random.randint(1, 20)])

        

        # Define column names

        cols = ['ProductKey_stdcost', 'OrderDateKey', 'ResellerKey', 'EmployeeKey', 'PromotionKey', 'CurrencyKey',

                'SalesTerritoryKey', 'SalesOrderNumber', 'SalesOrderLineNumber', 'RevisionNumber', 'OrderQuantity']

        

        # Create DataFrame from the generated data

        df1 = self.spark.createDataFrame(l).toDF(*cols)

        

        # Define UDFs for custom calculations

        @udf(FloatType())

        def freight_amt():

            return random.uniform(0.0344, 697.3405)

        

        CarrierTrackingNumber = ['6E46-440A-B5', 'B501-448E-96', '8551-4CDF-A1', 'B65C-4867-86', 'D530-40FA-B3', 'A60D-4721-BF', '5043-4C8B-B3', 'E366-4FF3-B2']

        CustomerPONumber = ['PO17835170901', 'PO12615187380', 'PO13340115824', 'PO10179176559', 'PO13050131387', 'PO16588183754', 'PO4930142451']

        

        @udf()

        def CustomerPONumber_udf():

            return random.choice(CustomerPONumber)

        

        @udf()
        def CarrierTrackingNumber_udf():

            return random.choice(CarrierTrackingNumber)

        

        @udf(FloatType())

        def UnitPrice_udf():

            return random.uniform(1.3282, 2146.962)

        

        # Applying UDFs and transformations to create new DataFrame 'new_df'

        new_df = df1.withColumn("UnitPrice", UnitPrice_udf()) \
                  .withColumn("ExtendedAmount", col('UnitPrice') * col("OrderQuantity")) \
                  .withColumn("UnitPriceDiscountPct", expr(

                "CASE "

                "WHEN PromotionKey IN (17, 18) THEN 0.20 "

                "WHEN PromotionKey = 19 THEN 0.00 "

                "WHEN PromotionKey = 3 THEN 0.05 "

                "WHEN PromotionKey = 4 THEN 0.1 "

                "WHEN PromotionKey = 7 THEN 0.35 "

                "WHEN PromotionKey IN (2, 6) THEN 0.02 "

                "ELSE 0.0 "

                "END"

            )) \
            .withColumn("DiscountAmount", (col('ExtendedAmount') * col('UnitPriceDiscountPct')).cast('Float')) \
            .withColumn("ProductKey", col("ProductKey_stdcost._1")) \
            .withColumn("ProductStandardCost", col("ProductKey_stdcost._2")) \
            .withColumn('TotalProductCost', (col('OrderQuantity') * col('ProductStandardCost')).cast('Float')) \
            .withColumn('SalesAmount', (col('ExtendedAmount') - col('DiscountAmount')).cast('Float')) \
            .withColumn("TaxAmt", col("SalesAmount") * 0.12) \
            .withColumn("Freight", freight_amt()) \
            .withColumn('OrderDateKey1', to_date(col('OrderDateKey').cast("string"), "yyyyMMdd")) \
            .withColumn('OrderDate', date_format(col('OrderDateKey1'), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
            .withColumn("DueDate", date_format(date_add(col('OrderDateKey1'), 15), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
            .withColumn("ShipDate", date_format(date_add(col('OrderDateKey1'), 10), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
            .withColumn("DueDateKey", col("OrderDateKey") + 15) \
            .withColumn("ShipDateKey", col("OrderDateKey") + 10)
        

        # Select the desired columns and create the final DataFrame 'df'

        df = new_df.select('ProductKey', 'OrderDateKey', 'DueDateKey', 'ShipDateKey', 'ResellerKey', 'EmployeeKey',

                        'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey', 'SalesOrderNumber', 'SalesOrderLineNumber',

                        'RevisionNumber', 'OrderQuantity', 'UnitPrice', 'ExtendedAmount', 'UnitPriceDiscountPct',

                        'DiscountAmount', 'ProductStandardCost', 'TotalProductCost', 'SalesAmount', 'TaxAmt', 'Freight', 'OrderDate', 'DueDate', 'ShipDate')
        
        return df