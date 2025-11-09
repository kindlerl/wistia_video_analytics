import streamlit as st
import pandas as pd
import boto3
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import plotly.express as px
from datetime import datetime

# -----------------------------------------------
# CONFIG
# -----------------------------------------------
ATHENA_DATABASE = "wistia_gold"
ATHENA_TABLE = "media_metrics_summary"
S3_OUTPUT = "s3://rlk-wistia-video-analytics-dev/queries/"
REGION = "us-east-1"
LOCAL_FALLBACK = "sample_gold_data.csv"

# -----------------------------------------------
# LOAD DATA
# -----------------------------------------------
@st.cache_data(ttl=600)
def load_data():
    try:
        # Attempt to read directly from Athena
        st.info("Querying Athena (live data)...")
        conn = connect(s3_staging_dir=S3_OUTPUT, region_name=REGION, cursor_class=PandasCursor)
        query = f"SELECT * FROM {ATHENA_DATABASE}.{ATHENA_TABLE};"
        df = pd.read_sql(query, conn)
        st.success(f"Loaded {len(df)} records from Athena.")
    except Exception as e:
        st.warning(f"Athena query failed: {e}")
        st.info("Falling back to local CSV sample data.")
        df = pd.read_csv(LOCAL_FALLBACK)
    return df

df = load_data()

# -----------------------------------------------
# PAGE SETUP
# -----------------------------------------------
st.set_page_config(page_title="🎬 Wistia Video Analytics Dashboard", layout="wide")

st.title("🎬 Wistia Video Analytics Dashboard")
st.markdown("""
A daily snapshot of media engagement metrics aggregated from Wistia API data.  
_Data sourced from AWS Glue (Gold layer) and visualized via Streamlit._
""")

# -----------------------------------------------
# FILTERS
# -----------------------------------------------
with st.sidebar:
    st.header("🔍 Filters")
    dates = sorted(df["load_date"].unique())
    selected_dates = st.multiselect("Select Date(s):", dates, default=dates[-1:])
    df_filtered = df[df["load_date"].isin(selected_dates)]

# -----------------------------------------------
# KPIs
# -----------------------------------------------
col1, col2, col3 = st.columns(3)
col1.metric("🎥 Total Plays", int(df_filtered["total_plays"].sum()))
col2.metric("👥 Total Visitors", int(df_filtered["total_visitors"].sum()))
col3.metric("📈 Avg Play Rate (%)", round(df_filtered["avg_play_rate"].mean(), 2))

st.divider()

# -----------------------------------------------
# CHARTS
# -----------------------------------------------

# 1️⃣ Plays by Media
fig1 = px.bar(
    df_filtered,
    x="media_name",
    y="total_plays",
    color="media_name",
    title="Total Plays by Media",
    text_auto=True
)
st.plotly_chart(fig1, use_container_width=True)

# 2️⃣ Engagement Trend
if "load_date" in df.columns:
    df_trend = df.groupby("load_date")[["avg_play_rate", "avg_engagement"]].mean().reset_index()
    fig2 = px.line(
        df_trend,
        x="load_date",
        y=["avg_play_rate", "avg_engagement"],
        markers=True,
        title="Play Rate & Engagement Trend Over Time"
    )
    st.plotly_chart(fig2, use_container_width=True)

# 3️⃣ Top 5 by Engagement
df_top = df_filtered.nlargest(5, "avg_engagement")
fig3 = px.bar(
    df_top,
    x="avg_engagement",
    y="media_name",
    orientation="h",
    title="Top 5 Videos by Engagement",
    text_auto=True
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()
st.caption(f"Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

