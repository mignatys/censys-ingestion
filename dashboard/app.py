import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
import os

# Configuration
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8001")
INGESTOR_URL = os.getenv("INGESTOR_URL", "http://localhost:8000")

st.set_page_config(page_title="Ingestion Dashboard", layout="wide")
st.title("🛡️ Security Alert Ingestion Dashboard")

# --- Sidebar: Configuration ---
st.sidebar.header("System Configuration")

# Fetch Current Config
current_failure = 0.2
current_interval = 30

try:
    # Fetch Ingestor Config
    config_resp = requests.get(f"{INGESTOR_URL}/config", timeout=1)
    if config_resp.status_code == 200:
        c = config_resp.json()
        current_interval = c.get("sync_interval_minutes", 30)

    # Fetch Mock API Config
    try:
         mock_resp = requests.get(f"{MOCK_API_URL}/config", timeout=1)
         if mock_resp.status_code == 200:
             m = mock_resp.json()
             current_failure = m.get("failure_rate", 0.2)
    except Exception:
         pass
except Exception:
    st.sidebar.warning("Could not fetch current config. Using defaults.")

st.sidebar.subheader("Chaos Engineering (Mock API)")
failure_rate = st.sidebar.slider("Failure Probability", 0.0, 1.0, float(current_failure), 0.05)

st.sidebar.subheader("Ingestion Schedule")
refresh_interval = st.sidebar.number_input("Sync Interval (minutes)", min_value=1, max_value=60, value=int(current_interval))

if st.sidebar.button("Apply Configuration"):
    success = True
    
    # 1. Update Mock API (Failure Rate)
    try:
        mock_payload = {"failure_rate": failure_rate}
        resp = requests.post(f"{MOCK_API_URL}/config", json=mock_payload)
        if resp.status_code != 200:
            st.sidebar.error(f"Mock API Update Failed: {resp.text}")
            success = False
    except Exception as e:
        st.sidebar.error(f"Mock API Error: {e}")
        success = False

    # 2. Update Ingestion Service (Sync Interval)
    try:
        ingest_payload = {"sync_interval_minutes": int(refresh_interval)}
        resp = requests.post(f"{INGESTOR_URL}/config", json=ingest_payload)
        if resp.status_code != 200:
            st.sidebar.error(f"Ingestor Update Failed: {resp.text}")
            success = False
    except Exception as e:
        st.sidebar.error(f"Ingestor Error: {e}")
        success = False
        
    if success:
        st.sidebar.success("Configuration saved & applied!")
        time.sleep(1)
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.header("Control")
if st.sidebar.button("Trigger Sync Now"):
    try:
        resp = requests.post(f"{INGESTOR_URL}/sync")
        if resp.status_code == 200:
            st.sidebar.info("Sync triggered!")
        else:
            st.sidebar.error(f"Sync failed: {resp.status_code}")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# --- Main Area ---

# 1. Health Monitoring
st.header("Service Health")
col1, col2 = st.columns(2)

# Poll Health
try:
    health_resp = requests.get(f"{INGESTOR_URL}/health", timeout=2)
    health_data = health_resp.json()
    # status = health_data.get("status", "Unknown") # Removed
    last_sync = health_data.get("last_sync", "Never")
    last_success = health_data.get("last_success", "Never")
    db_status = health_data.get("db_status", "Unknown")
    system_status = health_data.get("system_status", "Unknown")
    # ingestor_status = health_data.get("ingestor_status", "idle") # Removed, merged into system_status
    
    # Format Date
    if last_sync and last_sync != "Never":
        try:
             # Parse ISO format (e.g. 2026-01-28T18:24:31.123456+00:00)
             dt = pd.to_datetime(last_sync)
             # Format as Human Readable Date Space Time (YYYY-MM-DD HH:MM:SS)
             last_sync = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
             pass # Keep original if parse fails
    
    # Format Last Success
    if last_success and last_success != "Never":
        try:
             # Parse ISO format (e.g. 2026-01-28T18:24:31.123456+00:00)
             dt = pd.to_datetime(last_success)
             # Format as Human Readable Date Space Time (YYYY-MM-DD HH:MM:SS)
             last_success = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
             pass # Keep original if parse fails
    
    # Ingestor Status merged into system_status
    # System Status Logic (Left Column)
    with col1:
        if system_status == "retrying":
             st.warning(f"**System Status:** Retrying (Transient Error)...")
        elif system_status == "syncing":
             st.info(f"**System Status:** Syncing...")
        elif system_status == "failed" or system_status == "system_failure":
             st.error(f"**System Status:** Failed")
        elif system_status == "Unknown":
            st.error(f"**System Status:** Unknown (Check Service)")
        else:
             st.success(f"**System Status:** Success")
        
        st.metric("DB Connection", str(db_status))
            
    with col2:
        st.metric("Last Sync Time", str(last_sync))
        st.metric("Last Successful Sync", str(last_success))

except Exception as e:
    st.error(f"Cannot reach Ingestion Service: {e}")

st.markdown("---")

# --- Sidebar: Filters ---
st.sidebar.markdown("---")
st.sidebar.header("Data Filters")
time_window = st.sidebar.slider("Time Window (Hours)", 1, 24, 1)
filter_country = st.sidebar.text_input("Country Code (e.g. US)")
filter_criticality = st.sidebar.selectbox("Criticality / Severity", ["All", "low", "medium", "high", "critical"])
filter_user = st.sidebar.text_input("User ID")

# --- Main Area ---

# 1. Health Monitoring
st.header("Service Health")
col1, col2 = st.columns(2)

# 2. Data Visualization
st.header("Alerts Data")

try:
    # Build Params
    params = {"limit": 500, "hours": time_window}
    if filter_country:
        params["country"] = filter_country
    if filter_user:
        params["user"] = filter_user
    if filter_criticality != "All":
        params["criticality"] = filter_criticality

    # Fetch alerts
    alerts_resp = requests.get(f"{INGESTOR_URL}/alerts", params=params)
    if alerts_resp.status_code == 200:
        alerts = alerts_resp.json()
        
        if alerts:
            df = pd.DataFrame(alerts)
            
            # KPI Metrics
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Alerts (DB)", len(df))
            critical_count = len(df[df['severity'] == 'critical'])
            m2.metric("Critical Alerts", critical_count)
            
            # Visuals
            st.subheader("Recent Alerts")
            
            # Highlight Criticals
            def highlight_critical(row):
                return ['background-color: red' if row['severity'] == 'critical' else '' for _ in row]

            st.dataframe(df.style.apply(highlight_critical, axis=1), width='stretch')
            
            # Charts
            st.subheader("Alerts by Source")
            if 'source' in df.columns:
                counts = df['source'].value_counts().reset_index()
                counts.columns = ['source', 'count']
                fig = px.bar(counts, x='source', y='count', color='source', title="Distribution by Source")
                try: 
                    st.plotly_chart(fig, width='stretch') # type: ignore
                except:
                    # Fallback if specific version mismatch
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alerts found in database.")
    else:
        st.error(f"Failed to fetch alerts: {alerts_resp.status_code}")

except Exception as e:
    st.error(f"Error fetching alerts: {e}")

# --- Auto Refresh ---
time.sleep(5)
st.rerun()
