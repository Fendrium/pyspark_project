# Project file structure
# .
# ├── README.txt
# ├── clean_data.ipynb
# ├── clean_data.py
# └── data
#     ├── output
#     ├── raw
#     │ └── log20170201.csv
#     └── staging
#         └── clean_data.csv

import polars as pl
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn

# Load data in polars datframe
po = pl.read_csv("data/raw/log20170201.csv")

# Create datetime column from date and time
po = po.with_columns(
    pl.concat_str(
        [pl.col("date"), pl.col("time")],
        separator=" ",
    )
    .str.to_datetime()
    .alias("datetime"),
)
# Rename extention to extension
po = po.rename({"extention": "extension"})

# Reorder columns and remove previous date and time columns, and columns unused in further analysis
po = po.select(
    [
        "ip",
        "datetime",
        "extension",
        "size",
    ]
)

# Save data in staging
po.write_csv("data/staging/clean_data.csv")

# Init spark session and load CSV data into spark dataframe
spark = (
    SparkSession.builder.appName("Pyspark Session")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "10g")
    .getOrCreate()
)
df = spark.read.csv("data/staging/clean_data.csv", header=True)

# Convert datetime to timestamp
df_timestamp = df.withColumn(
    "timestamp", fn.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")
)

# Sessionize by 30 minute window for each ip address
# Also Aggregate filesize and file extension
sessions = (
    df_timestamp.withWatermark("datetime", "30 minutes")
    .groupBy("ip", fn.session_window("datetime", "30 minutes"))
    .agg(
        fn.sum("size").alias("total_filesize"),
        fn.count("extension").alias("total_downloads"),
    )
    .alias("num_connections_in_session")
)
sessions = sessions.withColumn("session_id", fn.monotonically_increasing_id())
sessions = sessions.select(
    "session_id", "ip", "session_window", "total_filesize", "total_downloads"
)

# top 10 sessions by total size of downloaded documents
sessions.sort(fn.col("total_filesize").desc()).show(10)

# top 10 sessions by total number of downloaded documents
sessions.sort(fn.col("total_downloads").desc()).show(10)
