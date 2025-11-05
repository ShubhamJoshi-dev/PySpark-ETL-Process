from dataclasses import dataclass


@dataclass
class ETLConfig:
    app_name: str = "Customer_Orders_ETL"
    input_path: str = "../data/input"
    output_path: str = "../data/output"
    master: str = "local[*]"
    shuffle_partitions: int = 4
    log_level: str = "ERROR"
    
    customers_file: str = "customers.csv"
    orders_file: str = "orders.csv"
    
    premium_threshold: float = 1500.0
    standard_threshold: float = 500.0

