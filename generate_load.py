from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count
import pandas as pd
import random
import time

# Initialize Spark session with Cassandra connector package
spark = SparkSession.builder \
    .appName("EcommerceDataPipeline") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .getOrCreate()

# Paths for the data files
user_actions_path = "data/user_actions.csv"
sales_data_path = "data/sales_data.csv"
inventory_updates_path = "data/inventory_updates.csv"

# Define data generation functions (reused from previous phases)
def append_user_actions(num_records=10):
    actions = ["view_product", "add_to_cart", "checkout", "search"]
    users = [f"user_{i}" for i in range(1, 21)]
    new_data = {
        "user_id": [random.choice(users) for _ in range(num_records)],
        "action": [random.choice(actions) for _ in range(num_records)],
        "timestamp": [time.time() for _ in range(num_records)]
    }
    df = pd.DataFrame(new_data)
    df.to_csv(user_actions_path, mode='a', header=False, index=False)

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
    df.to_csv(sales_data_path, mode='a', header=False, index=False)

def append_inventory_updates(num_records=5):
    products = [f"product_{i}" for i in range(1, 11)]
    new_data = {
        "product_id": [random.choice(products) for _ in range(num_records)],
        "quantity": [random.randint(0, 100) for _ in range(num_records)],
        "timestamp": [time.time() for _ in range(num_records)]
    }
    df = pd.DataFrame(new_data)
    df.to_csv(inventory_updates_path, mode='a', header=False, index=False)

# Simulate periodic batch processing and storing in Cassandra
for i in range(5):  # Simulate 5 batch loads
    print(f"\nBatch {i+1}: Processing and storing data...")

    # Append new data to CSV files and load into DataFrames
    append_user_actions()
    append_sales_data()
    append_inventory_updates()

    user_actions_df = spark.read.csv(user_actions_path, header=True, inferSchema=True)
    sales_data_df = spark.read.csv(sales_data_path, header=True, inferSchema=True)
    inventory_updates_df = spark.read.csv(inventory_updates_path, header=True, inferSchema=True)

    # Process data as in previous phase
    user_actions_summary = user_actions_df.groupBy("user_id", "action") \
                                          .agg(count("action").alias("action_count"))

    sales_summary = sales_data_df.groupBy("user_id") \
                                 .agg(spark_sum("amount").alias("total_sales"),
                                      avg("amount").alias("avg_order_value"))

    inventory_summary = inventory_updates_df.groupBy("product_id") \
                                            .agg(spark_sum("quantity").alias("total_quantity"))

    # Write transformed data to Cassandra
    user_actions_summary.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="user_actions_summary", keyspace="ecommerce_data") \
        .mode("append") \
        .save()

    sales_summary.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sales_summary", keyspace="ecommerce_data") \
        .mode("append") \
        .save()

    inventory_summary.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="inventory_summary", keyspace="ecommerce_data") \
        .mode("append") \
        .save()

    print(f"Batch {i+1} data stored in Cassandra.")
    time.sleep(10)  # Simulate interval between batches