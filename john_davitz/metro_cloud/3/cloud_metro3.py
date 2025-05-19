import os
import requests
import glob
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from google.cloud import storage

# === Spark Session Setup ===

spark = SparkSession.builder \
    .appName("MetroAPI") \
    .config("spark.driver.memory", "24g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

# === API Configuration ===
url = "https://data.ny.gov/resource/wujg-7c2s.json"
limit = 1000000
max_rows = int((110696370/4)*3) # For testing; use full count later
nyc_token = "wwBwMlUXanS8k91CYV0vJ3NlT"
 

# === Threaded Fetch Function ===
def fetch_and_process(offset):
    try:
        this_url = (
            f"{url}?$limit={limit}&$offset={offset}&$order=:id"
            f"&$$app_token={nyc_token}"
            f"&$where=transit_timestamp > '2023-01-01T00:00:00'"
        )
        print(f"Fetching offset: {offset:,}")
        response = requests.get(this_url)
        if response.status_code != 200:
            print(f"Error at offset {offset}: {response.status_code}")
            return None

        data = response.json()
        if not data:
            return None

        df = spark.read.json(spark.sparkContext.parallelize(data))

        df = df.select("transit_timestamp", "transit_mode", "borough", "payment_method", "ridership") \
               .filter(col("borough").isNotNull()) \
               .filter(col("borough") == "Manhattan") \
               .filter(col("transit_timestamp") >= "2020-01-01")

        print(f"Completed offset {offset:,}")
        return df

    except Exception as e:
        print(f"Exception at offset {offset}: {e}")
        return None

# === Parallel Fetch with Batching & Union Reduction ===
offsets = list(range(int((110696370/4)*2), max_rows, limit))
batch_size = 5
max_workers = 10
intermediate_dfs = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(fetch_and_process, offset) for offset in offsets]

    batch = []
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            batch.append(result)

        if len(batch) == batch_size or i == len(futures):
            print(f"Unioning batch of {len(batch)} DataFrames")
            unioned = batch[0]
            for df in batch[1:]:
                unioned = unioned.union(df)
            intermediate_dfs.append(unioned)
            batch = []

if not intermediate_dfs:
    print("No data collected.")
    exit(1)

print(f"Combining {len(intermediate_dfs)} intermediate DataFrames...")
combined_df = intermediate_dfs[0]
for df in intermediate_dfs[1:]:
    combined_df = combined_df.union(df)

# === Transform ===
combined_df = combined_df.withColumn("date", to_date(col("transit_timestamp"))) \
                         .withColumn("ridership", col("ridership").cast("double"))

result_df = combined_df.groupBy("date").sum("ridership") \
                       .withColumnRenamed("sum(ridership)", "ridership")

# === Write to local CSV ===
local_tmp_dir = "/tmp/metro_output"
final_csv_path = "/tmp/metro_api_output3.csv"

result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_tmp_dir)

# Move Spark output to expected path
csv_file = glob.glob(f"{local_tmp_dir}/part-*.csv")[0]
shutil.move(csv_file, final_csv_path)

# === Upload to Google Cloud Storage ===
client = storage.Client()
bucket = client.bucket("api_output_bucket_inst_final")
blob = bucket.blob("output/metro_api_output3.csv")

with open(final_csv_path, "rb") as f:
    blob.upload_from_file(f, content_type="text/csv")

print("✅ Upload complete.")
