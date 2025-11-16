# -----------------------------------------------------------
# Wistia Video Analytics Dashboard (FR11)
# -----------------------------------------------------------
# Author: Rich Kindle
# Description:
#   Interactive Streamlit dashboard displaying Gold-layer metrics
#   from the Wistia Video Analytics pipeline.
# -----------------------------------------------------------

import streamlit as st
import pandas as pd
import plotly.express as px
from utils import load_data, safe_numeric, summarize_kpis, trend_indicator, to_float_safe
from charts import top_media_chart, engagement_playrate_trend_chart, horizontal_funnel_chart

# -----------------------------------------------------------
# DUAL APPLICTION SETUP
# To make this application serve a dual purpose - that is, to
# be able to run this application normally through
# streamlit run app.py
# AND
# to run inside a jupyter notebook for a finer debugging
# environment, move all UI and Streamlit calls into a single
# main() function, then only run Streamlit UI if called 
# directly.
# -----------------------------------------------------------
def main():
    # -----------------------------------------------------------
    # PAGE SETUP
    # -----------------------------------------------------------
    st.set_page_config(
        page_title="Wistia Video Analytics Dashboard",
        page_icon="🎥",
        layout="wide"
    )

    st.title("🎬 Wistia Video Analytics Dashboard")
    with st.expander("ℹ️ About These Metrics"):
        st.markdown(
            """
            <div style="font-size: 1.1rem; color: #cccccc; margin-bottom: 1rem;">
                <p>
                    This dashboard visualizes aggregated video engagement metrics generated  
                    by the AWS-based Wistia Data Pipeline  
                </p>
                <p>
                    <strong>Note:</strong> Wistia’s Stats API returns <em>cumulative lifetime totals</em> 
                    for each video. Metrics shown here (Plays, Visitors, Play Rate, Engagement) represent 
                    the video’s overall performance up to the selected snapshot date — not the values for just that day.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )

    df = load_data()

    st.sidebar.header("Filters")

    # -----------------------------------------------------------
    # DATE SELECTION
    # -----------------------------------------------------------
    if "load_date" in df.columns:
        available_dates = sorted(df["load_date"].astype(str).unique())
    else:
        available_dates = []

    # Default to the latest load_date
    default_date = available_dates[-1] if available_dates else None

    selected_date = st.sidebar.selectbox(
        "Select Load Date:",
        options=available_dates,
        index=len(available_dates) - 1 if available_dates else 0,
        help="Choose a specific load_date to explore lifetime KPIs up to that day."
    )

    # -----------------------------------------------------------
    # FILTERS
    # -----------------------------------------------------------
    unique_media = [m for m in df["media_name"].unique() if m != "\\N"]

    if unique_media:
        selected_media = st.sidebar.selectbox("Select Media", unique_media)
        df_filtered = df[df["media_name"] == selected_media]
        df_filtered = df[
            (df["media_name"] == selected_media) &
            (df["load_date"] == selected_date)
        ]

        
    else:
        st.sidebar.info("No valid media names available.")
        df_filtered = df

    # -----------------------------------------------------------
    # FILTERED DATA BY SELECTED DATE
    # -----------------------------------------------------------
    df_selected = df_filtered[df_filtered["load_date"] == pd.to_datetime(selected_date)]

    # Determine previous date (for comparison)
    date_index = available_dates.index(selected_date)
    previous_date = available_dates[date_index - 1] if date_index > 0 else None

    df_previous = (
        df_filtered[df_filtered["load_date"] == previous_date]
        if previous_date
        else pd.DataFrame(columns=df.columns)
    )

    kpi_current = summarize_kpis(df_selected)
    kpi_previous = summarize_kpis(df_previous) if not df_previous.empty else kpi_current

    # -----------------------------------------------------------
    # KPI SUMMARY WITH TRENDS
    # -----------------------------------------------------------
    # st.subheader(f"📊 Key Performance Indicators (as of {current_date})")
    st.subheader(f"📊 Key Performance Indicators — {selected_date}")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Plays",
        kpi_current["total_plays"],
        # trend_indicator(
        #     to_float_safe(kpi_current["total_plays"].replace(",", "")),
        #     to_float_safe(kpi_previous["total_plays"].replace(",", ""))
        # )
    )

    col2.metric(
        "Total Visitors",
        kpi_current["total_visitors"],
        # trend_indicator(
        #     to_float_safe(kpi_current["total_visitors"].replace(",", "")),
        #     to_float_safe(kpi_previous["total_visitors"].replace(",", ""))
        # )
    )

    col3.metric(
        "Avg Play Rate (%)",
        kpi_current["avg_play_rate"],
        # trend_indicator(
        #     to_float_safe(kpi_current["avg_play_rate"]),
        #     to_float_safe(kpi_previous["avg_play_rate"])
        # )
    )

    col4.metric(
        "Avg Engagement (%)",
        kpi_current["avg_engagement"],
        # trend_indicator(
        #     to_float_safe(kpi_current["avg_engagement"]),
        #     to_float_safe(kpi_previous["avg_engagement"])
        # )
    )

    # -----------------------------------------------------------
    # VISUALIZATIONS
    # -----------------------------------------------------------
    st.divider()
    st.subheader("🎞️ Plays vs Visitors by Media")
    
    # --------------------------------
    # Funnel Chart - Lifetime metrics 
    # --------------------------------
    st.plotly_chart(horizontal_funnel_chart(df_filtered), use_container_width=True)

    st.divider()
    st.subheader("📈 Lifetime Engagement & Play Rate (Snapshot)")

    highlight_date = pd.to_datetime(selected_date, errors="coerce")
    # if pd.notna(highlight_date):
    #     highlight_date = pd.Timestamp(highlight_date)

    st.plotly_chart(engagement_playrate_trend_chart(df_filtered, highlight_date), use_container_width=True)


    # -----------------------------------------------------------
    # FOOTER
    # -----------------------------------------------------------
    st.divider()
    st.markdown(
        """
        **Data Source:** AWS Glue → S3 (Gold Layer)  
        **Author:** Rich Kindle  
        **Stack:** AWS Glue, PySpark, S3, Athena, Streamlit
        """
    )

# -----------------------------------------------------------
# Run Streamlit UI ONLY if called directly
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
    