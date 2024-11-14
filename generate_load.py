from pyspark.sql import SparkSession
import pandas as pd
import random
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataIngestion") \
    .getOrCreate()

# Paths for the data files
user_actions_path = "data/user_actions.csv"
sales_data_path = "data/sales_data.csv"
inventory_updates_path = "data/inventory_updates.csv"

# Define data generation functions
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

# Simulate periodic batch loading with CSV updates
for i in range(5):  # Simulate 5 batch loads
    print(f"\nBatch {i+1}: Generating and loading new data...")

    # Generate new data and append to CSVs
    append_user_actions()
    append_sales_data()
    append_inventory_updates()

    # Load the updated CSV files into Spark DataFrames
    user_actions_df = spark.read.csv(user_actions_path, header=True, inferSchema=True)
    print(f"User Actions Batch {i+1}:")
    user_actions_df.show(5)

    sales_data_df = spark.read.csv(sales_data_path, header=True, inferSchema=True)
    print(f"Sales Data Batch {i+1}:")
    sales_data_df.show(5)

    inventory_updates_df = spark.read.csv(inventory_updates_path, header=True, inferSchema=True)
    print(f"Inventory Updates Batch {i+1}:")
    inventory_updates_df.show(5)

    # Wait for the next batch (simulate real-time interval)
    time.sleep(10)