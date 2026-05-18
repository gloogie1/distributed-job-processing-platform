from pathlib import Path
import pandas as pd

DATA_DIR = Path("sample-data")
DATA_DIR.mkdir(exist_ok=True)

MONTHS = [
    "2024-01",
    "2024-02",
    "2024-03",
]

frames = []

for month in MONTHS:
    source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    print(f"Reading {source_url}")

    df = pd.read_parquet(source_url)

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

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df["vendor_id"] = pd.to_numeric(df["vendor_id"], errors="coerce").fillna(0).astype(int)
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(0).astype(int)
    df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").fillna(0).astype(int)

    print(f"{month}: {len(df):,} rows")
    frames.append(df)

combined = pd.concat(frames, ignore_index=True)

output_path = DATA_DIR / "yellow_tripdata_2024_q1.csv"
combined.to_csv(output_path, index=False)

print(f"Wrote {output_path}")
print(f"Total rows: {len(combined):,}")