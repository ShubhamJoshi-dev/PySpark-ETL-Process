from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    upper, trim, to_date, year, month, when
)
from pyspark.sql.types import DoubleType
from Config.config import ETLConfig


class DataCleaner:
    
    @staticmethod
    def clean_customers(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("first_name", upper(trim(col("first_name")))) \
            .withColumn("last_name", upper(trim(col("last_name")))) \
            .withColumn("email", trim(col("email"))) \
            .withColumn("registration_date", to_date(col("registration_date")))
    
    @staticmethod
    def clean_orders(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("order_date", to_date(col("order_date"))) \
            .withColumn("price", col("price").cast(DoubleType())) \
            .withColumn("quantity", col("quantity").cast("integer"))


class DataFilter:
    
    @staticmethod
    def filter_completed_orders(df: DataFrame) -> DataFrame:
        return df.filter(col("status") == "completed")


class DataEnricher:
    
    @staticmethod
    def add_calculated_columns(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("total_amount", spark_round(col("price") * col("quantity"), 2)) \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date")))


class DataAggregator:
    
    @staticmethod
    def create_customer_summary(df: DataFrame) -> DataFrame:
        return df \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                spark_round(spark_sum("total_amount"), 2).alias("total_spent"),
                spark_round(avg("total_amount"), 2).alias("avg_order_value"),
                count(when(col("status") == "completed", 1)).alias("completed_orders")
            ) \
            .orderBy(col("total_spent").desc())
    
    @staticmethod
    def create_product_summary(df: DataFrame) -> DataFrame:
        return df \
            .groupBy("product_name") \
            .agg(
                spark_sum("quantity").alias("total_quantity_sold"),
                count("order_id").alias("number_of_orders"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue")
            ) \
            .orderBy(col("total_revenue").desc())
    
    @staticmethod
    def create_country_sales(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        return orders_df \
            .join(customers_df, "customer_id") \
            .groupBy("country") \
            .agg(
                count("order_id").alias("total_orders"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                count("customer_id").alias("unique_customers")
            ) \
            .orderBy(col("total_revenue").desc())


class DataJoiner:
    
    @staticmethod
    def create_enriched_orders(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        return orders_df \
            .join(customers_df, "customer_id", "left") \
            .select(
                "order_id", "customer_id", "first_name", "last_name", "email",
                "country", "product_name", "quantity", "price", "total_amount",
                "order_date", "status"
            )


class CustomerSegmenter:
    
    def __init__(self, config: ETLConfig):
        self.config = config
    
    def segment_customers(self, summary_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        return summary_df \
            .join(customers_df, "customer_id") \
            .withColumn(
                "customer_segment",
                when(col("total_spent") >= self.config.premium_threshold, "Premium")
                .when(
                    (col("total_spent") >= self.config.standard_threshold) & 
                    (col("total_spent") < self.config.premium_threshold), 
                    "Standard"
                )
                .otherwise("Basic")
            ) \
            .select(
                "customer_id", "first_name", "last_name", "email", "country",
                "total_orders", "total_spent", "avg_order_value", "customer_segment"
            )


class DataTransformer:
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.cleaner = DataCleaner()
        self.filter = DataFilter()
        self.enricher = DataEnricher()
        self.aggregator = DataAggregator()
        self.joiner = DataJoiner()
        self.segmenter = CustomerSegmenter(config)
    
    def transform(self, customers_df: DataFrame, orders_df: DataFrame) -> dict[str, DataFrame]:
        customers_clean = self.cleaner.clean_customers(customers_df)
        orders_clean = self.cleaner.clean_orders(orders_df)
        
        completed_orders = self.filter.filter_completed_orders(orders_clean)
        orders_with_total = self.enricher.add_calculated_columns(completed_orders)
        
        customer_summary = self.aggregator.create_customer_summary(orders_with_total)
        product_summary = self.aggregator.create_product_summary(orders_with_total)
        country_sales = self.aggregator.create_country_sales(orders_with_total, customers_clean)
        enriched_orders = self.joiner.create_enriched_orders(orders_with_total, customers_clean)
        customer_segments = self.segmenter.segment_customers(customer_summary, customers_clean)
        
        return {
            "customer_summary": customer_summary,
            "product_summary": product_summary,
            "country_sales": country_sales,
            "enriched_orders": enriched_orders,
            "customer_segments": customer_segments
        }

