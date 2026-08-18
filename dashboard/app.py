"""
=============================================================================
MOBILE NETWORK DASHBOARD — Streamlit
=============================================================================

WHAT THIS APP DOES:
  A live operational dashboard for the mobile network streaming pipeline.
  Reads from three Athena tables, surfaces KPIs and charts, refreshes only
  when the user clicks Refresh.

COMPATIBILITY NOTE (Streamlit 1.57):
  This version avoids API patterns that changed since 1.32 and renders each
  section inside try/except so any single failure shows an inline error
  instead of blanking the whole page. Charts are built with explicit
  pandas reshaping (pivot) rather than the color= shorthand, which behaves
  inconsistently across Streamlit versions.

HOW TO RUN LOCALLY:
  1. AWS credentials configured (aws configure)
  2. pip install -r requirements.txt
  3. streamlit run app.py
=============================================================================
"""

import streamlit as st
import pandas as pd
import traceback
import athena_client as ac

st.set_page_config(
    page_title="Mobile Network Dashboard",
    page_icon="📡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# SQL QUERIES
# ---------------------------------------------------------------------------
SQL_SIGNAL = """
SELECT operator, postal_code, average_signal,
       window_start, window_end, partition_hour
FROM mobile_network_aggregations.signal_strength_by_operator
ORDER BY window_start DESC
LIMIT 200
"""

SQL_GPS = """
SELECT provider, average_precision, average_satellites,
       window_start, window_end, partition_hour
FROM mobile_network_aggregations.gps_precision_by_provider
ORDER BY window_start DESC
LIMIT 200
"""

SQL_STATUS = """
SELECT status, postal_code, status_count,
       window_start, window_end, partition_hour
FROM mobile_network_aggregations.status_count
ORDER BY window_start DESC
LIMIT 200
"""

# ---------------------------------------------------------------------------
# CACHED LOADERS
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Loading signal data...")
def load_signal() -> pd.DataFrame:
    df = ac.run_query(SQL_SIGNAL)
    return ac.coerce_types(
        df,
        numeric_cols=["average_signal"],
        timestamp_cols=["window_start", "window_end"],
    )

@st.cache_data(ttl=3600, show_spinner="Loading GPS data...")
def load_gps() -> pd.DataFrame:
    df = ac.run_query(SQL_GPS)
    return ac.coerce_types(
        df,
        numeric_cols=["average_precision", "average_satellites"],
        timestamp_cols=["window_start", "window_end"],
    )

@st.cache_data(ttl=3600, show_spinner="Loading status data...")
def load_status() -> pd.DataFrame:
    df = ac.run_query(SQL_STATUS)
    return ac.coerce_types(
        df,
        numeric_cols=["status_count"],
        timestamp_cols=["window_start", "window_end"],
    )

# ---------------------------------------------------------------------------
# HEADER + REFRESH
# ---------------------------------------------------------------------------
col_title, col_refresh = st.columns([4, 1])
with col_title:
    st.title("📡 Mobile Network Dashboard")
    st.caption("Real-time aggregates from Kinesis → Glue Streaming → Athena")
with col_refresh:
    st.write("")
    st.write("")
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ---------------------------------------------------------------------------
# LOAD DATA (with visible error handling)
# ---------------------------------------------------------------------------
try:
    df_signal = load_signal()
    df_gps    = load_gps()
    df_status = load_status()
except Exception as e:
    st.error("Failed to load data from Athena.")
    st.code(traceback.format_exc())
    st.stop()

st.success(f"Loaded {len(df_signal)} signal rows, {len(df_gps)} GPS rows, {len(df_status)} status rows.")

# ---------------------------------------------------------------------------
# KPI ROW
# ---------------------------------------------------------------------------
st.subheader("Key indicators")
try:
    k1, k2, k3, k4 = st.columns(4)
    if not df_signal.empty:
        best  = df_signal.loc[df_signal["average_signal"].idxmax()]
        worst = df_signal.loc[df_signal["average_signal"].idxmin()]
        k1.metric("Best operator", str(best["operator"]), f"signal {best['average_signal']:.1f}")
        k2.metric("Worst operator", str(worst["operator"]), f"signal {worst['average_signal']:.1f}")
    if not df_gps.empty:
        best_gps = df_gps.loc[df_gps["average_precision"].idxmin()]
        k3.metric("Most accurate GPS", str(best_gps["provider"]), f"{best_gps['average_precision']:.1f}m")
    if not df_signal.empty:
        latest = df_signal["window_start"].max()
        k4.metric("Latest window", latest.strftime("%H:%M:%S") if pd.notna(latest) else "—")
except Exception:
    st.warning("Could not render KPIs.")
    st.code(traceback.format_exc())

st.divider()

# ---------------------------------------------------------------------------
# SIGNAL STRENGTH CHARTS
# ---------------------------------------------------------------------------
# WHY PIVOT INSTEAD OF color="operator"?
# Streamlit's native charts render multiple series by having one COLUMN per
# series (wide format), which is version-stable. The color= shorthand (long
# format) changed behaviour across versions and is the likely cause of the
# blank page. Pivoting to wide format sidesteps it entirely.
st.subheader("Signal strength by operator")
try:
    if not df_signal.empty:
        # Average across postal codes per (window, operator), then pivot wide
        wide = (
            df_signal
            .groupby(["window_start", "operator"], as_index=False)["average_signal"]
            .mean()
            .pivot(index="window_start", columns="operator", values="average_signal")
            .sort_index()
        )
        st.line_chart(wide, height=320)

        # Bar: overall average per operator
        bar = (
            df_signal.groupby("operator")["average_signal"]
            .mean()
            .sort_values(ascending=True)
        )
        st.bar_chart(bar, height=240)
    else:
        st.info("No signal data.")
except Exception:
    st.warning("Could not render signal charts.")
    st.code(traceback.format_exc())

st.divider()

# ---------------------------------------------------------------------------
# GPS CHARTS
# ---------------------------------------------------------------------------
st.subheader("GPS precision by provider")
try:
    if not df_gps.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Average precision (m) — lower is better**")
            prec = (
                df_gps.groupby(["window_start", "provider"], as_index=False)["average_precision"]
                .mean()
                .pivot(index="window_start", columns="provider", values="average_precision")
                .sort_index()
            )
            st.line_chart(prec, height=240)
        with c2:
            st.markdown("**Average satellites**")
            sats = (
                df_gps.groupby(["window_start", "provider"], as_index=False)["average_satellites"]
                .mean()
                .pivot(index="window_start", columns="provider", values="average_satellites")
                .sort_index()
            )
            st.line_chart(sats, height=240)
    else:
        st.info("No GPS data.")
except Exception:
    st.warning("Could not render GPS charts.")
    st.code(traceback.format_exc())

st.divider()

# ---------------------------------------------------------------------------
# RAW DATA TABLES
# ---------------------------------------------------------------------------
# WHY NO .style.format()?
# The pandas Styler object behaves differently across Streamlit versions and
# can fail to render. Plain DataFrames with pre-rounded values are stable.
st.subheader("Recent windows (raw data)")
try:
    t1, t2, t3 = st.tabs(["Signal strength", "GPS precision", "Status count"])
    with t1:
        d = df_signal.head(50).copy()
        if "average_signal" in d.columns:
            d["average_signal"] = d["average_signal"].round(2)
        st.dataframe(d, use_container_width=True, hide_index=True)
    with t2:
        d = df_gps.head(50).copy()
        for c in ("average_precision", "average_satellites"):
            if c in d.columns:
                d[c] = d[c].round(2)
        st.dataframe(d, use_container_width=True, hide_index=True)
    with t3:
        st.dataframe(df_status.head(50), use_container_width=True, hide_index=True)
except Exception:
    st.warning("Could not render data tables.")
    st.code(traceback.format_exc())

st.divider()
st.caption("Kinesis → Glue Structured Streaming → S3 Parquet → Athena → Streamlit")