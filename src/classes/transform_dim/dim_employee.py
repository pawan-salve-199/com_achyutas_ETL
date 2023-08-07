from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.utils import *

class DimEmployee():
    def __init__(self,**kwargs):
        self.logger=kwargs.get("logger")
        self.spark=kwargs.get("spark")
        self.emp_df=kwargs.get("emp_df")
        self.date_df=kwargs.get("date_df")
        self.sales_teritory_dF=kwargs.get("sales_teritory_dF")

    def transform(self):
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
            empDF = self.emp_df
            datedf = self.date_df
            salesteritoryDF = self.sales_teritory_dF
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