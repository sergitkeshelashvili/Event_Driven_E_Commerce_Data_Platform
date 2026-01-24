from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("Ecommerce_Full_Medallion_Pipeline") \
    .config("spark.jars", "/mnt/c/Users/sergi/OneDrive/Desktop/ecommerce_project/postgresql-42.6.0.jar") \
    .getOrCreate()

# 2. Config
source_url = "jdbc:postgresql://localhost:5432/postgres"
dest_url = "jdbc:postgresql://localhost:5432/e_commerce_db"
db_props = {"user": "postgres", "password": "123999", "driver": "org.postgresql.Driver"}

# --- STEP 1: SOURCE TO BRONZE (Ingestion)

print("--- Moving data: Source (public) -> Bronze ---")
raw_data_df = spark.read.jdbc(url=source_url, table='"public"."order_events"', properties=db_props)

raw_data_df.write.mode("overwrite").jdbc(url=dest_url, table="bronze.order_events", properties=db_props)

# --- STEP 2: BRONZE TO SILVER (Cleaning)

print("--- Moving data: Bronze -> Silver ---")
bronze_df = spark.read.jdbc(url=dest_url, table="bronze.order_events", properties=db_props)

silver_df = bronze_df.dropDuplicates(["event_id"])
silver_df.write.mode("overwrite").jdbc(url=dest_url, table="silver.order_events", properties=db_props)

# --- STEP 3: SILVER TO GOLD (Summary/State) ---
print("--- Moving data: Silver -> Gold Summary ---")
latest_window = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())

summary_df = silver_df.withColumn("rn", F.row_number().over(latest_window)) \
    .filter(F.col("rn") == 1) \
    .select(
        "order_id",
        F.col("event_type").alias("current_status"),
        "items",
        F.col("amount").alias("total_amount"),
        F.col("event_time").alias("last_updated_at")
    )

summary_df.write.mode("overwrite").jdbc(url=dest_url, table="gold.order_summary", properties=db_props)

# --- STEP 4: SILVER TO GOLD (Performance/KPIs) ---
print("--- Moving data: Silver -> Gold Performance ---")
performance_df = silver_df.groupBy("order_id").agg(
    F.min("event_time").alias("order_placed_at"),
    F.max("event_time").alias("order_completed_at"),
    F.count("event_id").alias("total_events"),
    (F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))).alias("cycle_time_seconds")
)
performance_df.write.mode("overwrite").jdbc(url=dest_url, table="gold.order_performance", properties=db_props)

# --- STEP 5: GOLD SUMMARY TO GOLD FACT (Exploded Items) ---
print("--- Moving data: Gold Summary -> Gold Items Fact ---")
items_fact_df = summary_df.withColumn(
    "product_name",
    F.explode(F.from_json(F.col("items"), ArrayType(StringType())))
).select(
    "order_id",
    "product_name",
    F.col("last_updated_at").alias("sale_timestamp")
)

items_fact_df.write.mode("overwrite").jdbc(url=dest_url, table="gold.order_items_fact", properties=db_props)

print("Full Medallion Pipeline Completed: Source -> Bronze -> Silver -> Gold")
spark.stop()
