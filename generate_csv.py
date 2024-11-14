import pandas as pd

# Define headers for each file
user_actions_columns = ["user_id", "action", "timestamp"]
sales_data_columns = ["user_id", "product_id", "amount", "timestamp"]
inventory_updates_columns = ["product_id", "quantity", "timestamp"]

# Create empty CSVs with headers
pd.DataFrame(columns=user_actions_columns).to_csv("data/user_actions.csv", index=False)
pd.DataFrame(columns=sales_data_columns).to_csv("data/sales_data.csv", index=False)
pd.DataFrame(columns=inventory_updates_columns).to_csv("data/inventory_updates.csv", index=False)