from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, window
import pandas as pd
import random
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataProcessing") \
    .getOrCreate()

# Paths for the data files
user_actions_path = "data/user_actions.csv"
sales_data_path = "data/sales_data.csv"
inventory_updates_path = "data/inventory_updates.csv"

# Define data generation functions (reused from Phase 1)
def append_user_actions(num_records=10):
    actions = ["view_product", "add_to_cart", "checkout", "search"]
    users = [f"user_{i}" for i in range(1, 21)]
    new_data = {
        "user_id": [random.choice(users) for _ in range(num_records)],
        "action": [random.choice(actions) for _ in range(num_records)],
        "timestamp": [time.time() for _ in range(num_records)]
    }
    df = pd.DataFrame(new_data)
    df.to_csv(user_actions_path, mode='a', header=False, index=False)  # Append to CSV

def append_sales_data(num_records=10):
    products = [f"product_{i}" for i in range(1, 11)]
    users = [f"user_{i}" for i in range(1, 21)]
    new_data = {
        "user_id": [random.choice(users) for _ in range(num_records)],
        "product_id": [random.choice(products) for _ in range(num_records)],
        "amount": [round(random.uniform(10, 200), 2) for _ in range(num_records)],
        "timestamp": [time.time() for _ in range(num_records)]
    }
    df = pd.DataFrame(new_data)
    df.to_csv(sales_data_path, mode='a', header=False, index=False)  # Append to CSV

def append_inventory_updates(num_records=5):
    products = [f"product_{i}" for i in range(1, 11)]
    new_data = {
        "product_id": [random.choice(products) for _ in range(num_records)],
        "quantity": [random.randint(0, 100) for _ in range(num_records)],
        "timestamp": [time.time() for _ in range(num_records)]
    }
    df = pd.DataFrame(new_data)
    df.to_csv(inventory_updates_path, mode='a', header=False, index=False)  # Append to CSV

# Simulate periodic batch loading and processing
for i in range(5):  # Simulate 5 batch loads
    print(f"\nBatch {i+1}: Generating and loading new data...")

    # Append new data to CSV files
    append_user_actions()
    append_sales_data()
    append_inventory_updates()

    # Load updated CSVs into DataFrames
    user_actions_df = spark.read.csv(user_actions_path, header=True, inferSchema=True)
    sales_data_df = spark.read.csv(sales_data_path, header=True, inferSchema=True)
    inventory_updates_df = spark.read.csv(inventory_updates_path, header=True, inferSchema=True)

    # --- Data Transformations ---
    
    # 1. User Actions Transformation: Count actions per user
    user_actions_summary = user_actions_df.groupBy("user_id", "action") \
                                          .agg(count("action").alias("action_count")) \
                                          .orderBy("user_id")
    print(f"\nUser Actions Summary for Batch {i+1}:")
    user_actions_summary.show(5)

    # 2. Sales Data Transformation: Calculate total and average sales per user
    sales_summary = sales_data_df.groupBy("user_id") \
                                 .agg(spark_sum("amount").alias("total_sales"),
                                      avg("amount").alias("avg_order_value")) \
                                 .orderBy("user_id")
    print(f"\nSales Summary for Batch {i+1}:")
    sales_summary.show(5)

    # 3. Inventory Updates Transformation: Calculate total quantity per product
    inventory_summary = inventory_updates_df.groupBy("product_id") \
                                            .agg(spark_sum("quantity").alias("total_quantity")) \
                                            .orderBy("product_id")
    print(f"\nInventory Summary for Batch {i+1}:")
    inventory_summary.show(5)

    # Wait for the next batch (simulate real-time interval)
    time.sleep(10)