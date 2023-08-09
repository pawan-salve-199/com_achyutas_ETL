from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class FactServeResponse():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.sales_res_df=kwargs.get("sales_res_df")
        self.prod_cat_df=kwargs.get("prod_cat_df")
        self.prod_sub_cat_df=kwargs.get("prod_sub_cat_df")
        self.customer_df=kwargs.get("customer_df")
        
    def transform(self):
        FSalesRes=self.sales_res_df
        ProCate=self.prod_cat_df
        proSubCate=self.prod_sub_cat_df
        customer=self.customer_df

        max_id = FSalesRes.select(max("SurveyResponseKey")).collect()[0][0]

        cust_range=customer.select(min("CustomerKey").alias("min_cust"), max("CustomerKey").alias("max_cust")).collect()[0]

        @udf()
        def cust_id_udf():
            return random.randint(cust_range[0],cust_range[1])

        ProCate =ProCate.withColumnRenamed("ProductCategoryKey","ProductCategoryKey_pk")

        joindf=ProCate.join(proSubCate , ProCate.ProductCategoryKey_pk == proSubCate.ProductCategoryKey)

        new_df =joindf.select('ProductCategoryKey','EnglishProductCategoryName','ProductSubcategoryKey','EnglishProductSubcategoryName')

        list_values= new_df.rdd.map(list).collect()


        new_list= [random.choice(list_values) for i in range(FSalesRes.count())]
        df2 = self.spark.createDataFrame(new_list,['ProductCategoryKey','EnglishProductCategoryName','ProductSubcategoryKey','EnglishProductSubcategoryName'])

        fact_df = df2.withColumn("SurveyResponseKey", max_id + row_number().over(Window.orderBy(monotonically_increasing_id()))).withColumn("DateKey",date_format(current_date(), "yyyyMMdd")).withColumn("Date", date_format(current_date(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")).withColumn("CustomerKey",cust_id_udf())

        final_fact_df = FSalesRes.unionByName(fact_df)

        max_id = final_fact_df.select(max("SurveyResponseKey")).collect()[0][0]
        sample_df=final_fact_df.sample(withReplacement=False,fraction=0.25)
        new_df=sample_df.withColumn("SurveyResponseKey",max_id + row_number().over(Window.orderBy(monotonically_increasing_id()))).withColumn("DateKey",date_format(current_date(), "yyyyMMdd")).withColumn("Date", date_format(current_date(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

        final_fact_df = final_fact_df.union(new_df)

        return final_fact_df
    




