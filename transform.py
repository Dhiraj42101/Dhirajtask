"""
Task 3 -- Transformation Layer (Silver)
Reads raw CSVs, applies data quality rules, and writes cleaned parquet files
back to S3 under silver/ prefix. Also writes local copies to data/silver/.

Run: python src/transform.py
"""

import boto3
import pandas as pd
import numpy as np
import io
import os
from datetime import datetime

# -- CONFIG ---------------------------------------------------------
S3_BUCKET  = "smart-facility-dhiraj"          # <-- UPDATE THIS
AWS_REGION = "ap-south-1"
RAW_PREFIX = "raw"
SILVER_PREFIX = "silver"
LOCAL_DATA    = "data"
LOCAL_SILVER  = "data/silver"
# ------------------------------------------------------------------

os.makedirs(LOCAL_SILVER, exist_ok=True)


def read_csv_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Download CSV from S3 and return as DataFrame."""
    print(f"  Reading  s3://{bucket}/{key} ...", end=" ")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(f"{len(df):,} rows")
    return df


def write_parquet_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str, local_path: str):
    """Write DataFrame as parquet to S3 and locally."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    df.to_parquet(local_path, index=False, engine="pyarrow")
    print(f"  Written  s3://{bucket}/{key}  ({len(df):,} rows)  OK")


# ==================================================================
# SILVER -- SITE MASTER
# ==================================================================
def transform_sites(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[silver_sites]")
    before = len(df)

    # Deduplicate on site_id
    df = df.drop_duplicates(subset=["site_id"])

    # Standardise string columns
    df["site_name"]     = df["site_name"].str.strip()
    df["city"]          = df["city"].str.strip().str.title()
    df["region"]        = df["region"].str.strip().str.title()
    df["site_category"] = df["site_category"].str.strip().str.lower()

    # Validate category
    valid_cats = {"warehouse", "office", "retail"}
    df["site_category"] = df["site_category"].where(
        df["site_category"].isin(valid_cats), other="unknown"
    )

    # Add load timestamp
    df["_silver_loaded_at"] = datetime.utcnow().isoformat()

    print(f"  Rows: {before} → {len(df)}  (removed {before - len(df)} duplicates)")
    return df


# ==================================================================
# SILVER -- DEVICES
# ==================================================================
def transform_devices(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[silver_devices]")
    before = len(df)

    # Deduplicate on device_id (keep last)
    df = df.drop_duplicates(subset=["device_id"], keep="last")

    # Standardise string columns
    df["device_type"]      = df["device_type"].str.strip().str.lower()
    df["firmware_version"] = df["firmware_version"].str.strip()
    df["status"]           = df["status"].str.strip().str.lower()

    # Validate device_type
    valid_types = {"camera", "temperature_sensor", "motion_sensor", "gateway"}
    df["device_type"] = df["device_type"].where(
        df["device_type"].isin(valid_types), other="unknown"
    )

    # Validate status
    valid_statuses = {"active", "inactive", "maintenance"}
    df["status"] = df["status"].where(
        df["status"].isin(valid_statuses), other="unknown"
    )

    # Parse install_date
    df["install_date"] = pd.to_datetime(df["install_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df["_silver_loaded_at"] = datetime.utcnow().isoformat()
    print(f"  Rows: {before} → {len(df)}  (removed {before - len(df)} duplicates)")
    return df


# ==================================================================
# SILVER -- SENSOR EVENTS
# ==================================================================
def transform_events(df: pd.DataFrame, silver_devices: pd.DataFrame) -> pd.DataFrame:
    print("\n[silver_sensor_events]")
    before = len(df)

    # -- 1. Remove exact duplicates ------------------------------
    df = df.drop_duplicates()
    after_dedup = len(df)
    print(f"  After dedup          : {before:,} → {after_dedup:,}  (removed {before - after_dedup:,})")

    # -- 2. Parse timestamps -------------------------------------
    df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
    null_ts = df["event_ts"].isna().sum()
    df = df.dropna(subset=["event_ts"])
    print(f"  Dropped null ts      : {null_ts}")

    # -- 3. Standardise alert_type -------------------------------
    valid_alert_types = {"battery_low", "temp_high", "offline", "motion_detected"}
    df["alert_type"] = df["alert_type"].str.strip().str.lower()
    df["alert_type"] = df["alert_type"].where(
        df["alert_type"].isin(valid_alert_types), other=None
    )

    # -- 4. Handle null battery_pct ------------------------------
    # Flag nulls, then impute with median per site_id
    df["battery_pct_is_null"] = df["battery_pct"].isna()
    median_battery = df.groupby("site_id")["battery_pct"].transform("median")
    df["battery_pct"] = df["battery_pct"].fillna(median_battery)
    df["battery_pct"] = df["battery_pct"].fillna(df["battery_pct"].median())  # fallback
    imputed = df["battery_pct_is_null"].sum()
    print(f"  Imputed battery_pct  : {imputed:,} values (site median)")

    # -- 5. Fix invalid signal_strength -------------------------
    # Valid range: -120 to -20 dBm
    valid_signal_mask = df["signal_strength"].between(-120, -20)
    invalid_signal = (~valid_signal_mask).sum()
    df["signal_strength_is_invalid"] = ~valid_signal_mask
    df["signal_strength"] = df["signal_strength"].where(valid_signal_mask, other=np.nan)
    print(f"  Nulled invalid signal: {invalid_signal:,} values")

    # -- 6. Clip temperature to realistic range ------------------
    df["temperature_c"] = df["temperature_c"].clip(lower=-50, upper=100)

    # -- 7. Add derived columns ----------------------------------
    df["event_date"] = df["event_ts"].dt.strftime("%Y-%m-%d")
    df["event_hour"] = df["event_ts"].dt.hour

    # Flag events from inactive/maintenance devices
    non_active = set(silver_devices[silver_devices["status"] != "active"]["device_id"])
    df["device_is_inactive"] = df["device_id"].isin(non_active)

    df["_silver_loaded_at"] = datetime.utcnow().isoformat()

    print(f"  Final rows           : {len(df):,}")
    return df


# ==================================================================
# MAIN
# ==================================================================
def main():
    print(f"\n{'-'*55}")
    print(f"  Smart Facility -- Transformation (Silver Layer)")
    print(f"  Bucket  : s3://{S3_BUCKET}")
    print(f"  Time    : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'-'*55}")

    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"\n[OK] Connected to S3 bucket: {S3_BUCKET}")
    except Exception as e:
        print(f"[ERROR] S3 connection failed: {e}")
        print("  Falling back to local CSV files in ./data/")
        s3 = None

    # -- Read raw data --
    print("\n-- Reading raw layer --------------------------")
    if s3:
        raw_sites   = read_csv_from_s3(s3, S3_BUCKET, f"{RAW_PREFIX}/site_master/site_master.csv")
        raw_devices = read_csv_from_s3(s3, S3_BUCKET, f"{RAW_PREFIX}/devices/devices.csv")
        raw_events  = read_csv_from_s3(s3, S3_BUCKET, f"{RAW_PREFIX}/sensor_events/sensor_events.csv")
    else:
        raw_sites   = pd.read_csv(f"{LOCAL_DATA}/site_master.csv")
        raw_devices = pd.read_csv(f"{LOCAL_DATA}/devices.csv")
        raw_events  = pd.read_csv(f"{LOCAL_DATA}/sensor_events.csv")
        print(f"  site_master   : {len(raw_sites):,} rows  (local)")
        print(f"  devices       : {len(raw_devices):,} rows  (local)")
        print(f"  sensor_events : {len(raw_events):,} rows  (local)")

    # -- Transform --
    print("\n-- Transforming -------------------------------")
    silver_sites   = transform_sites(raw_sites)
    silver_devices = transform_devices(raw_devices)
    silver_events  = transform_events(raw_events, silver_devices)

    # -- Write silver layer --
    print("\n-- Writing silver layer -----------------------")
    if s3:
        write_parquet_to_s3(s3, silver_sites,   S3_BUCKET, f"{SILVER_PREFIX}/silver_sites/data.parquet",         f"{LOCAL_SILVER}/silver_sites.parquet")
        write_parquet_to_s3(s3, silver_devices, S3_BUCKET, f"{SILVER_PREFIX}/silver_devices/data.parquet",       f"{LOCAL_SILVER}/silver_devices.parquet")
        write_parquet_to_s3(s3, silver_events,  S3_BUCKET, f"{SILVER_PREFIX}/silver_sensor_events/data.parquet", f"{LOCAL_SILVER}/silver_sensor_events.parquet")
    else:
        # Local only
        silver_sites.to_parquet(  f"{LOCAL_SILVER}/silver_sites.parquet",          index=False)
        silver_devices.to_parquet(f"{LOCAL_SILVER}/silver_devices.parquet",        index=False)
        silver_events.to_parquet( f"{LOCAL_SILVER}/silver_sensor_events.parquet",  index=False)
        print(f"  silver_sites          → {LOCAL_SILVER}/silver_sites.parquet  OK")
        print(f"  silver_devices        → {LOCAL_SILVER}/silver_devices.parquet  OK")
        print(f"  silver_sensor_events  → {LOCAL_SILVER}/silver_sensor_events.parquet  OK")

    print(f"\n{'-'*55}")
    print("  Silver layer complete.")
    print(f"{'-'*55}\n")


if __name__ == "__main__":
    main()