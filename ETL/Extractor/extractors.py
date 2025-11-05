from pyspark.sql import SparkSession, DataFrame
from Config.config import ETLConfig


class DataExtractor:
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        self.spark = spark
        self.config = config
    
    def extract_customers(self) -> DataFrame:
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{self.config.input_path}/{self.config.customers_file}")
    
    def extract_orders(self) -> DataFrame:
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{self.config.input_path}/{self.config.orders_file}")
    
    def extract_all(self) -> tuple[DataFrame, DataFrame]:
        return self.extract_customers(), self.extract_orders()

