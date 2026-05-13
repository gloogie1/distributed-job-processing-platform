from pathlib import Path
import pandas as pd

DATA_DIR = Path("sample-data")
DATA_DIR.mkdir(exist_ok=True)

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

parquet_path = DATA_DIR / "yellow_tripdata_2024-01.parquet"
csv_path = DATA_DIR / "yellow_tripdata_2024-01.csv"

print("Downloading NYC TLC Yellow Taxi January 2024 parquet...")
df = pd.read_parquet(SOURCE_URL)

print(f"Rows loaded: {len(df):,}")
print("Columns:")
print(list(df.columns))

# Keep columns that match our current worker validation logic.
# The worker expects:
# vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,fare_amount,total_amount,payment_type
df = df.rename(columns={
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
})

df = df[
    [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "payment_type",
    ]
]

# Convert datetime columns to the format the Java worker expects.
df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

print(f"Writing CSV to {csv_path}...")
df["vendor_id"] = pd.to_numeric(df["vendor_id"], errors="coerce").fillna(0).astype(int)
df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(0).astype(int)
df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").fillna(0).astype(int)
df.to_csv(csv_path, index=False)

print("Done.")
print(f"CSV path: {csv_path}")
print(f"Rows written: {len(df):,}")