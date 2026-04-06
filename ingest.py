"""
Task 2 -- Raw Ingestion Layer
Uploads generated CSV files to AWS S3 under the raw/bronze layer.
Run: python src/ingest.py
Pre-requisite: AWS CLI configured (aws configure) and S3 bucket exists.
"""

import boto3
import os
import sys
from datetime import datetime

# -- CONFIG -- change bucket name to your own ----------------------
S3_BUCKET  = "smart-facility-dhiraj"          # <-- UPDATE THIS
AWS_REGION = "ap-south-1"                       # <-- UPDATE if needed
LOCAL_DATA = "data"
# -----------------------------------------------------------------

FILES = {
    "devices.csv":       "raw/devices/devices.csv",
    "site_master.csv":   "raw/site_master/site_master.csv",
    "sensor_events.csv": "raw/sensor_events/sensor_events.csv",
}


def upload_file(s3_client, local_path: str, s3_key: str, bucket: str) -> bool:
    """Upload a single file to S3 and print status."""
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"  Uploading  {local_path}  →  s3://{bucket}/{s3_key}  ({file_size_mb:.2f} MB) ...", end=" ")
    try:
        s3_client.upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "text/csv"}
        )
        print("OK")
        return True
    except Exception as e:
        print(f"FAIL  ERROR: {e}")
        return False


def main():
    print(f"\n{'-'*55}")
    print(f"  Smart Facility -- Raw Ingestion")
    print(f"  Bucket  : s3://{S3_BUCKET}")
    print(f"  Region  : {AWS_REGION}")
    print(f"  Time    : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'-'*55}\n")

    # Check local files exist
    missing = [f for f in FILES if not os.path.exists(os.path.join(LOCAL_DATA, f))]
    if missing:
        print(f"[ERROR] Missing local files: {missing}")
        print("  Run `python src/generate_data.py` first.")
        sys.exit(1)

    # Connect to S3
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"[OK] Connected to S3 bucket: {S3_BUCKET}\n")
    except Exception as e:
        print(f"[ERROR] Cannot connect to S3: {e}")
        print("  Check: bucket name, AWS credentials, region.")
        sys.exit(1)

    # Upload each file
    results = []
    for filename, s3_key in FILES.items():
        local_path = os.path.join(LOCAL_DATA, filename)
        ok = upload_file(s3, local_path, s3_key, S3_BUCKET)
        results.append((filename, s3_key, ok))

    # Summary
    print(f"\n{'-'*55}")
    success = sum(1 for _, _, ok in results if ok)
    print(f"  Uploaded {success}/{len(results)} files successfully.")
    if success == len(results):
        print("\n  S3 structure created:")
        for _, s3_key, _ in results:
            print(f"    s3://{S3_BUCKET}/{s3_key}")
    print(f"{'-'*55}\n")


if __name__ == "__main__":
    main()