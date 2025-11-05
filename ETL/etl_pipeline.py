from pyspark.sql import SparkSession
from Config.config import ETLConfig
from ETL.Extractor.extractors import DataExtractor
from ETL.Transformer.transformers import DataTransformer
from ETL.Loader.loaders import DataLoader


class SparkSessionManager:
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.spark = None
    
    def create_session(self) -> SparkSession:
        self.spark = SparkSession.builder \
            .appName(self.config.app_name) \
            .master(self.config.master) \
            .config("spark.sql.shuffle.partitions", str(self.config.shuffle_partitions)) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel(self.config.log_level)
        return self.spark
    
    def stop_session(self) -> None:
        if self.spark:
            self.spark.stop()


class ETLPipeline:
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.session_manager = SparkSessionManager(config)
        self.spark = None
        self.extractor = None
        self.transformer = None
        self.loader = None
    
    def setup(self) -> None:
        self.spark = self.session_manager.create_session()
        self.extractor = DataExtractor(self.spark, self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)
    
    def run(self) -> None:
        self.setup()
        
        try:
            customers_df, orders_df = self.extractor.extract_all()
            transformed_data = self.transformer.transform(customers_df, orders_df)
            self.loader.load_all(transformed_data)
        finally:
            self.session_manager.stop_session()


def main():
    config = ETLConfig()
    pipeline = ETLPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
