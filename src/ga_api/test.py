#Read data file from URI of secondary Azure Data Lake Storage Gen2
import pandas as pd
import json

with open("../../secrets/azure_creds.json", "r") as f:
    storage_options = json.load(f)

# file details
container = "staging"
account_name = "gruppotavolastorage"
file_path = "oracle/EmployeeItems_7606_2022-08-01.csv"

df = pd.read_csv(
    f"abfs://{container}@{account_name}.dfs.core.windows.net/{file_path}",
    storage_options=storage_options
)
print(df.head())