"""
Task 5 — Consumable Report
Generates a self-contained HTML dashboard from the silver parquet files.
This works LOCALLY (no cloud needed) and also doubles as the report
proof-of-output you can include in your Git repo.

Run: python src/report.py
Output: reports/smart_facility_report.html  (open in any browser)

For Power BI: see instructions at bottom of this file.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

SILVER_DIR  = "data/silver"
REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────
# Load silver data
# ─────────────────────────────────────────────────────────────────
def load_silver():
    print("Loading silver data...")
    events  = pd.read_parquet(f"{SILVER_DIR}/silver_sensor_events.parquet")
    devices = pd.read_parquet(f"{SILVER_DIR}/silver_devices.parquet")
    sites   = pd.read_parquet(f"{SILVER_DIR}/silver_sites.parquet")

    events["event_ts"]   = pd.to_datetime(events["event_ts"])
    events["event_date"] = pd.to_datetime(events["event_date"])
    events["event_hour"] = events["event_ts"].dt.hour

    print(f"  Events : {len(events):,}")
    print(f"  Devices: {len(devices):,}")
    print(f"  Sites  : {len(sites):,}")
    return events, devices, sites


# ─────────────────────────────────────────────────────────────────
# Compute gold-layer metrics (mirrors the SQL views)
# ─────────────────────────────────────────────────────────────────
def compute_metrics(events, devices, sites):
    # KPI cards
    total_events  = len(events)
    total_alerts  = events["alert_flag"].sum()
    alert_rate    = round(total_alerts / total_events * 100, 1)
    unique_devices = events["device_id"].nunique()

    # Top 5 devices by alert count
    top_devices = (
        events[events["alert_flag"]]
        .groupby("device_id")
        .size()
        .reset_index(name="alert_count")
        .sort_values("alert_count", ascending=False)
        .head(5)
        .merge(devices[["device_id", "device_type", "site_id"]], on="device_id", how="left")
    )

    # Site-wise daily event trend
    daily_trend = (
        events.groupby(["event_date", "site_id"])
        .size()
        .reset_index(name="event_count")
        .merge(sites[["site_id", "site_name"]], on="site_id", how="left")
        .sort_values("event_date")
    )

    # Average battery by device type
    battery_by_type = (
        events.merge(devices[["device_id", "device_type"]], on="device_id", how="left")
        .groupby("device_type")["battery_pct"]
        .mean()
        .round(1)
        .reset_index()
        .rename(columns={"battery_pct": "avg_battery_pct"})
        .sort_values("avg_battery_pct")
    )

    # Alert type breakdown
    alert_breakdown = (
        events[events["alert_type"].notna()]
        .groupby("alert_type")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    # Average battery by site
    battery_by_site = (
        events.groupby("site_id")["battery_pct"]
        .mean()
        .round(1)
        .reset_index()
        .rename(columns={"battery_pct": "avg_battery_pct"})
        .merge(sites[["site_id", "site_name"]], on="site_id", how="left")
        .sort_values("avg_battery_pct")
    )

    return {
        "total_events":    total_events,
        "total_alerts":    total_alerts,
        "alert_rate":      alert_rate,
        "unique_devices":  unique_devices,
        "top_devices":     top_devices,
        "daily_trend":     daily_trend,
        "battery_by_type": battery_by_type,
        "alert_breakdown": alert_breakdown,
        "battery_by_site": battery_by_site,
    }


# ─────────────────────────────────────────────────────────────────
# HTML Report
# ─────────────────────────────────────────────────────────────────
def build_html_report(m):
    """Render a standalone HTML dashboard using Chart.js."""

    # Prepare chart data as JSON-safe structures
    daily = m["daily_trend"]
    dates_all    = sorted(daily["event_date"].astype(str).unique().tolist())
    top5_sites   = (daily.groupby("site_name")["event_count"].sum()
                    .nlargest(5).index.tolist())
    daily_top5   = daily[daily["site_name"].isin(top5_sites)]

    COLORS = ["#4F87D4","#E8593C","#1D9E75","#EF9F27","#7F77DD"]

    # Build datasets for multi-line trend chart
    trend_datasets = []
    for idx, sname in enumerate(top5_sites):
        sdata = daily_top5[daily_top5["site_name"] == sname]
        date_map = dict(zip(sdata["event_date"].astype(str), sdata["event_count"]))
        values   = [date_map.get(d, 0) for d in dates_all]
        color    = COLORS[idx % len(COLORS)]
        trend_datasets.append({
            "label":            sname,
            "data":             values,
            "borderColor":      color,
            "backgroundColor":  color + "22",
            "tension":          0.3,
            "fill":             False,
            "pointRadius":      3,
        })

    # Top 5 devices bar chart data
    td = m["top_devices"]
    top_dev_labels = (td["device_id"] + " (" + td["device_type"].fillna("?") + ")").tolist()
    top_dev_values = td["alert_count"].tolist()

    # Battery by device type
    bt = m["battery_by_type"]
    bat_labels = bt["device_type"].tolist()
    bat_values = bt["avg_battery_pct"].tolist()

    # Alert breakdown doughnut
    ab = m["alert_breakdown"]
    ab_labels = ab["alert_type"].tolist()
    ab_values = ab["count"].tolist()

    # Battery by site
    bs = m["battery_by_site"]
    bs_labels = bs["site_name"].tolist()
    bs_values = bs["avg_battery_pct"].tolist()

    import json
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Smart Facility Monitoring — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
        background:#f5f5f0;color:#2c2c2a;padding:24px}}
  h1{{font-size:22px;font-weight:600;margin-bottom:4px}}
  .subtitle{{font-size:13px;color:#888;margin-bottom:24px}}
  .kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}}
  .kpi{{background:#fff;border-radius:12px;padding:20px;
        border:1px solid #e5e5e0;text-align:center}}
  .kpi .value{{font-size:32px;font-weight:700;color:#185FA5;margin:6px 0}}
  .kpi .label{{font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.5px}}
  .kpi .sub{{font-size:11px;color:#aaa;margin-top:4px}}
  .charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}}
  .chart-card{{background:#fff;border-radius:12px;padding:20px;
               border:1px solid #e5e5e0}}
  .chart-card.full{{grid-column:1/-1}}
  .chart-title{{font-size:14px;font-weight:500;margin-bottom:14px;color:#3d3d3a}}
  canvas{{max-height:260px}}
  footer{{text-align:center;font-size:11px;color:#aaa;margin-top:24px}}
</style>
</head>
<body>

<h1>Smart Facility Monitoring</h1>
<div class="subtitle">Generated: {now} &nbsp;|&nbsp; Last 7 days &nbsp;|&nbsp; All sites</div>

<!-- KPI Cards -->
<div class="kpi-row">
  <div class="kpi">
    <div class="label">Total Events</div>
    <div class="value">{m['total_events']:,}</div>
    <div class="sub">across {m['unique_devices']} devices</div>
  </div>
  <div class="kpi">
    <div class="label">Total Alerts</div>
    <div class="value" style="color:#E8593C">{m['total_alerts']:,}</div>
    <div class="sub">{m['alert_rate']}% alert rate</div>
  </div>
  <div class="kpi">
    <div class="label">Active Devices</div>
    <div class="value" style="color:#1D9E75">{m['unique_devices']}</div>
    <div class="sub">reporting in last 7d</div>
  </div>
  <div class="kpi">
    <div class="label">Alert Rate</div>
    <div class="value" style="color:#EF9F27">{m['alert_rate']}%</div>
    <div class="sub">events flagged as alert</div>
  </div>
</div>

<!-- Charts Row 1 -->
<div class="charts-grid">
  <div class="chart-card full">
    <div class="chart-title">Site-wise Daily Event Trend (top 5 sites)</div>
    <canvas id="trendChart"></canvas>
  </div>
</div>

<!-- Charts Row 2 -->
<div class="charts-grid">
  <div class="chart-card">
    <div class="chart-title">Top 5 Devices by Alert Count</div>
    <canvas id="topDevChart"></canvas>
  </div>
  <div class="chart-card">
    <div class="chart-title">Alert Type Breakdown</div>
    <canvas id="alertDonut"></canvas>
  </div>
</div>

<!-- Charts Row 3 -->
<div class="charts-grid">
  <div class="chart-card">
    <div class="chart-title">Average Battery Level by Device Type</div>
    <canvas id="battTypeChart"></canvas>
  </div>
  <div class="chart-card">
    <div class="chart-title">Average Battery Level by Site</div>
    <canvas id="battSiteChart"></canvas>
  </div>
</div>

<footer>Smart Facility Monitoring Dashboard &mdash; Data Engineering Machine Test</footer>

<script>
const COLORS = {json.dumps(COLORS)};

// Trend chart
new Chart(document.getElementById('trendChart'), {{
  type:'line',
  data:{{
    labels:{json.dumps(dates_all)},
    datasets:{json.dumps(trend_datasets)}
  }},
  options:{{responsive:true,interaction:{{mode:'index',intersect:false}},
    scales:{{y:{{beginAtZero:true,title:{{display:true,text:'Events'}}}}}},
    plugins:{{legend:{{position:'bottom'}}}}
  }}
}});

// Top devices bar
new Chart(document.getElementById('topDevChart'), {{
  type:'bar',
  data:{{
    labels:{json.dumps(top_dev_labels)},
    datasets:[{{label:'Alerts',data:{json.dumps(top_dev_values)},
      backgroundColor:COLORS,borderRadius:6}}]
  }},
  options:{{responsive:true,indexAxis:'y',
    plugins:{{legend:{{display:false}}}},
    scales:{{x:{{beginAtZero:true}}}}
  }}
}});

// Alert doughnut
new Chart(document.getElementById('alertDonut'), {{
  type:'doughnut',
  data:{{
    labels:{json.dumps(ab_labels)},
    datasets:[{{data:{json.dumps(ab_values)},
      backgroundColor:COLORS,borderWidth:2,borderColor:'#fff'}}]
  }},
  options:{{responsive:true,plugins:{{legend:{{position:'bottom'}}}}}}
}});

// Battery by device type
new Chart(document.getElementById('battTypeChart'), {{
  type:'bar',
  data:{{
    labels:{json.dumps(bat_labels)},
    datasets:[{{label:'Avg Battery %',data:{json.dumps(bat_values)},
      backgroundColor:'#4F87D4',borderRadius:6}}]
  }},
  options:{{responsive:true,scales:{{y:{{min:0,max:100,
    title:{{display:true,text:'Battery %'}}}}}},
    plugins:{{legend:{{display:false}}}}
  }}
}});

// Battery by site
new Chart(document.getElementById('battSiteChart'), {{
  type:'bar',
  data:{{
    labels:{json.dumps(bs_labels)},
    datasets:[{{label:'Avg Battery %',data:{json.dumps(bs_values)},
      backgroundColor:'#1D9E75',borderRadius:6}}]
  }},
  options:{{responsive:true,indexAxis:'y',
    scales:{{x:{{min:0,max:100,title:{{display:true,text:'Battery %'}}}}}},
    plugins:{{legend:{{display:false}}}}
  }}
}});
</script>
</body>
</html>"""
    return html


def main():
    events, devices, sites = load_silver()
    print("\nComputing metrics...")
    m = compute_metrics(events, devices, sites)

    print("\nBuilding HTML report...")
    html = build_html_report(m)
    out_path = f"{REPORTS_DIR}/smart_facility_report.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n[✓] Report saved → {out_path}")
    print("    Open it in any browser to view the dashboard.\n")


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════
# POWER BI CONNECTION INSTRUCTIONS (using AWS Athena ODBC)
# ═══════════════════════════════════════════════════════════════════
#
# 1. Download Simba Athena ODBC Driver:
#    https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html
#
# 2. Create a DSN (Windows ODBC Data Source Administrator):
#    - Driver     : Simba Athena ODBC Driver
#    - AwsRegion  : ap-south-1  (your region)
#    - S3OutputLoc: s3://<YOUR_BUCKET>/athena-results/
#    - AuthType   : IAM Credentials
#    - UID        : <your AWS Access Key ID>
#    - PWD        : <your AWS Secret Access Key>
#
# 3. In Power BI Desktop:
#    - Get Data → ODBC → your DSN
#    - Navigate to: smart_facility database
#    - Import these views/tables:
#        * vw_site_hourly_health
#        * vw_device_alert_summary
#        * vw_site_daily_kpis
#        * vw_abnormal_activity_24h
#
# 4. Build visuals:
#    - Card: Total Events (COUNT from vw_site_daily_kpis)
#    - Card: Total Alerts
#    - Bar chart: Top 5 devices by total_alerts (vw_device_alert_summary)
#    - Line chart: date vs total_events by site_name (vw_site_daily_kpis)
#    - Bar chart: avg_battery_pct by device_type (vw_device_alert_summary)
#
# ═══════════════════════════════════════════════════════════════════
