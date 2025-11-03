import os
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Evidence.dev - Spark KPIs", layout="wide")

st.title("Evidence.dev — Spark KPIs Dashboard")
st.markdown("This dashboard reads the parquet outputs produced by the SparkJob and displays simple charts and tables.")

st.sidebar.header("Environment")
env = os.environ.get("EVIDENCE_ENV", "(not set)")
st.sidebar.write(f"EVIDENCE_ENV = {env}")

DATA_ROOT = "/opt/spark-data/output"
KPIS_PATH = os.path.join(DATA_ROOT, "kpis")
WEEKLY_PATH = os.path.join(DATA_ROOT, "weekly_metrics")

st.sidebar.header("Data locations")
st.sidebar.write(KPIS_PATH)
st.sidebar.write(WEEKLY_PATH)

@st.cache_data(ttl=60)
def load_parquet(path):
    if not os.path.exists(path):
        return None
    try:
        # pandas can read a directory of parquet files with pyarrow
        df = pd.read_parquet(path)
        return df
    except Exception as e:
        st.sidebar.error(f"Error reading {path}: {e}")
        return None

kpis_df = load_parquet(KPIS_PATH)
weekly_df = load_parquet(WEEKLY_PATH)

if kpis_df is None and weekly_df is None:
    st.warning("No parquet outputs found. Run the Spark job to generate data at ./data/output/ (the dashboard mounts ./data).")
else:
    col1, col2 = st.columns(2)
    with col1:
        st.header("KPIs (by week & borough)")
        if kpis_df is None:
            st.info("KPIs dataset not found at: {}".format(KPIS_PATH))
        else:
            st.dataframe(kpis_df)

    with col2:
        st.header("Weekly Metrics (joined)")
        if weekly_df is None:
            st.info("Weekly metrics dataset not found at: {}".format(WEEKLY_PATH))
        else:
            st.dataframe(weekly_df)

    # Simple visualizations using weekly metrics (if available)
    if weekly_df is not None and not weekly_df.empty:
        st.subheader("Trip volume by borough (most recent week)")
        try:
            latest_week = weekly_df["week_start"].max()
            latest_df = weekly_df[weekly_df["week_start"] == latest_week]
            vol_by_borough = latest_df.groupby("borough")["trip_volume"].sum().sort_values(ascending=False)
            st.bar_chart(vol_by_borough)
        except Exception as e:
            st.error(f"Error building borough chart: {e}")

        st.subheader("Revenue share by borough (most recent week)")
        try:
            rev_by_borough = latest_df.groupby("borough")["total_revenue"].sum()
            st.pie_chart(rev_by_borough)
        except Exception as e:
            st.error(f"Error building revenue chart: {e}")

    # Display some top-level KPIs
    if kpis_df is not None and not kpis_df.empty:
        st.header("Top-level KPIs")
        try:
            total_trips = int(kpis_df['total_trips'].sum())
            total_revenue = float(kpis_df['total_revenue'].sum())
            avg_rev_per_mile = float(kpis_df['avg_revenue_per_mile'].mean())
            st.metric("Total Trips (window)", total_trips)
            st.metric("Total Revenue (window)", f"${total_revenue:,.2f}")
            st.metric("Avg Revenue / Mile", f"${avg_rev_per_mile:.2f}")
        except Exception as e:
            st.error(f"Error computing top-level KPIs: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("Dashboard built: Streamlit (reads parquet files under /opt/spark-data/output). Mount ./data when running.")
