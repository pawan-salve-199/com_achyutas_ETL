from pyspark.sql import SparkSession
import sys

class SparkClass:
    def __init__(self, logger_obj=None, config={},spark_session=None):
        """
        Initialize the SparkClass instance.

        Parameters:
            logger_obj (logging.Logger): Logger object for logging messages.
            config (dict, optional): Configuration for the Spark session (app_name, master, and spark_config).

        """
        self.app_name = config.get("app_name")
        self.master = config.get('master')
        self.spark_config = config.get("spark_config")
        self.spark = spark_session
        self.logger = logger_obj

    def start_spark(self):
        """
        Start the Spark session.

        Returns:
            pyspark.sql.SparkSession: Spark session object.

        """
        try:
            self.logger.info("Entering start_spark method.")
            self.spark = self.create_session()
            self.logger.info("Spark Session Initialized.")
            return self.spark
        except Exception as e:
            self.logger.error(f"Error starting Spark session: {str(e)}")
            sys.exit()

    def create_session(self):
        """
        Create a Spark session.

        Returns:
            pyspark.sql.SparkSession: Spark session object.

        """
        try:
            spark_builder = SparkSession.builder.appName(self.app_name)
            if self.spark_config:
                for key, val in self.spark_config.items():
                    spark_builder = spark_builder.config(key, val)
            spark = spark_builder.getOrCreate()
            self.logger.info("Configured the Spark for the specified src.dependencies")
            return spark
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {str(e)}")
            sys.exit()


    def disconnect(self):
        """
        Disconnect the Spark session.

        """
        try:
            if self.spark and not self.spark.sparkContext._jsc.sc().isStopped():
                self.spark.stop()
                self.logger.info("Spark session stopped.")
            else:
                self.logger.warning("Trying to disconnect when Spark session is not initialized.")
        except Exception as e:
            self.logger.error(f"Error while disconnecting Spark session: {str(e)}")
            sys.exit()

    def extract(self, file_path=None, file_format="jdbc", jdbc_params={}, **options):
        """
        Read data from a source.

        Parameters:
            file_path (str, optional): Path to the data file (local or HDFS).
            file_format (str, optional): Data file format ('parquet', 'json', 'csv', etc.).
            jdbc_params (dict, optional): JDBC connection parameters for reading data from a database.
            **options: Additional options specific to the data format.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing the data.

        Raises:
            ValueError: If the Spark session is not initialized.

        """
        try:
            if not isinstance(self.spark, SparkSession):
                raise ValueError("Spark session is not initialized. Please connect first.")
            
            if file_format.upper()=="JDBC":
                file_name=jdbc_params.get("table")
            else:
                file_name=file_path.split("\\")[-1]

            if file_format.upper() == "JDBC":
                jdbc_url = f"jdbc:sqlserver://localhost:{jdbc_params.get('localhost')};databaseName={jdbc_params.get('database')};trustServerCertificate=true;"
                jdbcDF = self.spark.read.format("jdbc").option("url", jdbc_url) \
                                    .option("dbtable", jdbc_params.get('table')) \
                                    .option("user", jdbc_params.get('user')) \
                                    .option("password", jdbc_params.get('password')) \
                                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                                    .load()
                self.logger.info(f"Data read from JDBC source for {jdbc_params.get('table')}")

                return jdbcDF
            
            else:
                df = self.spark.read.format(file_format).load(file_path, **options)
                self.logger.info(f"Data read from {file_name} file.")
            
                return df
        
        except Exception as e:
            self.logger.error(f"Error while reading data from : {str(e)}")
            sys.exit()

    def write_data(self, df, mode, file_path, file_format="parquet", **options):
        """
        Write data to a destination.

        Parameters:
            df (pyspark.sql.DataFrame): DataFrame to be written.
            mode (str): Write mode ('append', 'overwrite', etc.).
            file_path (str): Path to write the data (local or HDFS).
            file_format (str, optional): Data file format ('parquet', 'json', 'csv', etc.).
            **options: Additional options specific to the data format.

        Raises:
            ValueError: If the Spark session is not initialized.
        """
        try:
            if not isinstance(self.spark, SparkSession):
                raise ValueError("Spark session is not initialized. Please connect first.")

            if file_format == "jdbc":
                # Implement JDBC write logic here if required
                pass
            else:
                df.write.format(file_format).mode(mode).save(file_path, **options)
                file_name=file_path.split("\\")[-1]
                self.logger.info(f"Data written to {file_name} file.")
        except Exception as e:
            self.logger.error(f"Error while writing data: {str(e)}")
            sys.exit()
        
