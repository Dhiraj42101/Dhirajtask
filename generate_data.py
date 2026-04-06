"""
Task 1 — Data Generation
Smart Facility Monitoring — generate reproducible, realistic, imperfect source data.
Run: python src/generate_data.py
Output: data/devices.csv, data/site_master.csv, data/sensor_events.csv
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# 1) SITE MASTER  (15 sites)
# ─────────────────────────────────────────────
SITES = [
    ("SITE_01", "Mumbai Central",    "Mumbai",    "West",  "office"),
    ("SITE_02", "Pune Warehouse",    "Pune",      "West",  "warehouse"),
    ("SITE_03", "Delhi HQ",          "Delhi",     "North", "office"),
    ("SITE_04", "Chennai Retail",    "Chennai",   "South", "retail"),
    ("SITE_05", "Bangalore Tech",    "Bangalore", "South", "office"),
    ("SITE_06", "Kolkata Store",     "Kolkata",   "East",  "retail"),
    ("SITE_07", "Hyderabad Hub",     "Hyderabad", "South", "warehouse"),
    ("SITE_08", "Ahmedabad Plant",   "Ahmedabad", "West",  "warehouse"),
    ("SITE_09", "Jaipur Retail",     "Jaipur",    "North", "retail"),
    ("SITE_10", "Lucknow Office",    "Lucknow",   "North", "office"),
    ("SITE_11", "Surat Warehouse",   "Surat",     "West",  "warehouse"),
    ("SITE_12", "Kochi Hub",         "Kochi",     "South", "warehouse"),
    ("SITE_13", "Nagpur Retail",     "Nagpur",    "West",  "retail"),
    ("SITE_14", "Bhopal Office",     "Bhopal",    "Central","office"),
    ("SITE_15", "Indore Retail",     "Indore",    "Central","retail"),
]

site_master = pd.DataFrame(SITES, columns=[
    "site_id", "site_name", "city", "region", "site_category"
])
site_master.to_csv(f"{OUTPUT_DIR}/site_master.csv", index=False)
print(f"[OK] site_master.csv  -- {len(site_master)} rows")

# ─────────────────────────────────────────────
# 2) DEVICES  (200 devices)
# ─────────────────────────────────────────────
DEVICE_TYPES   = ["camera", "temperature_sensor", "motion_sensor", "gateway"]
FIRMWARE_VERS  = ["v1.0.2", "v1.1.0", "v1.2.5", "v2.0.0", "v2.1.3"]
STATUSES       = ["active", "inactive", "maintenance"]
STATUS_WEIGHTS = [0.75, 0.15, 0.10]

site_ids = [s[0] for s in SITES]

devices = []
for i in range(1, 201):
    device_id      = f"DEV_{i:04d}"
    site_id        = random.choice(site_ids)
    device_type    = random.choice(DEVICE_TYPES)
    install_date   = (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).date()
    firmware       = random.choice(FIRMWARE_VERS)
    status         = random.choices(STATUSES, STATUS_WEIGHTS)[0]
    devices.append([device_id, site_id, device_type, install_date, firmware, status])

devices_df = pd.DataFrame(devices, columns=[
    "device_id", "site_id", "device_type", "install_date", "firmware_version", "status"
])
devices_df.to_csv(f"{OUTPUT_DIR}/devices.csv", index=False)
print(f"[OK] devices.csv      -- {len(devices_df)} rows")

# ─────────────────────────────────────────────
# 3) SENSOR EVENTS  (~12,000 rows, last 7 days)
# ─────────────────────────────────────────────
END_TIME   = datetime(2024, 1, 14, 23, 59, 59)          # fixed end for reproducibility
START_TIME = END_TIME - timedelta(days=7)

ALERT_TYPES = ["battery_low", "temp_high", "offline", "motion_detected", None]

# Use all devices (including inactive — intentional imperfection)
all_device_ids = devices_df["device_id"].tolist()
device_site_map = dict(zip(devices_df["device_id"], devices_df["site_id"]))

events = []
for i in range(1, 12001):
    event_id        = f"EVT_{i:07d}"
    device_id       = random.choice(all_device_ids)
    site_id         = device_site_map[device_id]
    seconds_offset  = random.randint(0, int((END_TIME - START_TIME).total_seconds()))
    event_ts        = START_TIME + timedelta(seconds=seconds_offset)
    temperature_c   = round(random.uniform(15.0, 45.0), 2)
    alert_flag      = random.random() < 0.15                        # 15% alerts

    # alert_type — only set if alert_flag, else null
    if alert_flag:
        alert_type = random.choice(ALERT_TYPES[:-1])                # exclude None
    else:
        alert_type = None

    # ── Intentional imperfections ──────────────────────
    # ~8% missing battery values
    battery_pct = round(random.uniform(5.0, 100.0), 1) if random.random() > 0.08 else None

    # ~5% invalid signal strength (out-of-range: should be -120 to 0 dBm)
    if random.random() < 0.05:
        signal_strength = random.choice([999, -999, 0])             # invalid
    else:
        signal_strength = round(random.uniform(-100.0, -30.0), 1)   # valid dBm
    # ───────────────────────────────────────────────────

    events.append([
        event_id, event_ts, device_id, site_id,
        temperature_c, battery_pct, signal_strength,
        alert_flag, alert_type
    ])

events_df = pd.DataFrame(events, columns=[
    "event_id", "event_ts", "device_id", "site_id",
    "temperature_c", "battery_pct", "signal_strength",
    "alert_flag", "alert_type"
])

# ── Inject ~200 duplicate rows (intentional imperfection) ──
duplicate_sample = events_df.sample(n=200, random_state=SEED)
events_df = pd.concat([events_df, duplicate_sample], ignore_index=True)
events_df = events_df.sample(frac=1, random_state=SEED).reset_index(drop=True)  # shuffle

events_df.to_csv(f"{OUTPUT_DIR}/sensor_events.csv", index=False)
print(f"[OK] sensor_events.csv -- {len(events_df)} rows  (incl. ~200 duplicates)")

# ─────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────
print("\n-- Data quality summary ---------------------")
print(f"  Inactive devices still sending events : "
      f"{events_df[events_df['device_id'].isin(devices_df[devices_df['status']!='active']['device_id'])].shape[0]} events")
print(f"  Missing battery_pct                   : {events_df['battery_pct'].isna().sum()}")
print(f"  Invalid signal_strength rows          : {events_df[events_df['signal_strength'].isin([999,-999,0])].shape[0]}")
print(f"  Duplicate event rows injected         : ~200")
print(f"  Alert events                          : {events_df['alert_flag'].sum()}")
print("---------------------------------------------\n")
print("All files saved to ./data/")