from pyspark.sql import DataFrame
from config import ETLConfig
import os


class DataLoader:
    
    def __init__(self, config: ETLConfig):
        self.config = config
    
    def load_to_csv(self, df: DataFrame, name: str) -> None:
        output_file = f"{self.config.output_path}/{name}"
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_file)
    
    def load_all(self, transformed_data: dict[str, DataFrame]) -> None:
        os.makedirs(self.config.output_path, exist_ok=True)
        
        for name, df in transformed_data.items():
            self.load_to_csv(df, name)

