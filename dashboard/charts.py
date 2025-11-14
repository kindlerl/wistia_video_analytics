# -----------------------------------------------------------
# charts.py
# -----------------------------------------------------------
# Purpose:
#   Centralized chart functions for the Wistia Video Analytics Dashboard
# -----------------------------------------------------------

import plotly.express as px
import pandas as pd

def plays_vs_visitors_chart(df: pd.DataFrame):
    """
    Bar chart showing total plays vs total visitors for each media.
    """
    fig = px.bar(
        df,
        x="media_name",
        y=["total_plays", "total_visitors"],
        barmode="group",
        title="Total Plays vs Visitors by Media",
        labels={"value": "Count", "media_name": "Media"},
    )
    fig.update_layout(
        xaxis_title="Media",
        yaxis_title="Count",
        legend_title="Metric",
        template="plotly_white",
    )
    return fig


def engagement_playrate_trend_chart(df, highlight_date=None):
    # =============================================
    # Ensure proper sorting and datetime type
    df_sorted = df.sort_values("load_date").copy()
    df_sorted['load_date'] = pd.to_datetime(df_sorted["load_date"], errors="coerce")

    # fig = px.line (
    #     df_sorted,
    #     x="load_date",
    #     y=["avg_engagement", "avg_play_rate"],
    #     color_discrete_sequence=["#FF7F0E", "#1F77B4"],
    #     markers=True,
    #     title="Engagement vs Play Rate Trends Over Time",
    #     labels={
    #         "load_date": "Load Date",
    #         "value": "Percentage",
    #         "variable": "Metric",
    #     },
    # )
    fig = px.scatter(
        df_sorted,
        x="load_date",
        y=[
            "avg_engagement",
            "avg_play_rate"
        ],
        labels={
            "load_date": "Load Date",
            "value": "Percentage",
            "variable": "Metric",
        },
        title="Engagement vs Play Rate Trends Over Time",
    )

    fig.update_traces(mode="markers+lines")
    fig.update_traces(marker=dict(size=10))

    if highlight_date is not None and not pd.isna(highlight_date):
        # ✅ Convert to ISO string to avoid Timestamp arithmetic errors
        # highlight_str = pd.to_datetime(highlight_date).strftime("%Y-%m-%d")
        highlight_ts = pd.to_datetime(highlight_date).to_pydatetime().timestamp() * 1000  # milliseconds

        if len(df_sorted) > 1:
            fig.add_vline(
                x=highlight_ts,
                line_width=2,
                line_dash="dot",
                line_color="green",
                annotation_text=f"Selected Date ({pd.to_datetime(highlight_date).strftime('%Y-%m-%d')})",
                annotation_position="top right"
            )

    fig.update_layout(template="plotly_white")

    # Improve tooltips
    fig.update_traces(
        hovertemplate=(
            "%{y}%<br>"
            "Metric: %{fullData.name}<br>"
            "As of %{x|%b %d, %Y}<extra></extra>"
        )
    )
    return fig

def top_media_chart(df):
    df_top = df.sort_values("total_plays", ascending=False).head(10)
    fig = px.bar(
        df_top,
        x="media_name",
        y="total_plays",
        title="Top 10 Media by Total Plays",
        text_auto=True
    )
    return fig

def horizontal_funnel_chart(df: pd.DataFrame):
    """
    Creates two aligned horizontal bars (Plays vs Non-Playing Visitors).
    This visually represents the funnel exactly as shown in the example.
    """
    if df.empty:
        return px.bar(title="No data available for selected media.")

    media_name = df["media_name"].iloc[0]
    total_plays = int(df["total_plays"].sum())
    total_visitors = int(df["total_visitors"].sum())
    non_playing_visitors = total_visitors - total_plays

    funnel_df = pd.DataFrame({
        "metric": ["Plays", "Non-Playing Visitors"],
        "count": [total_plays, non_playing_visitors]
    })

    fig = px.bar(
        funnel_df,
        x="count",
        y="metric",
        orientation="h",
        text="count",
        labels={"count": "Count", "metric": ""},
        title=f"Visitor-to-Play Conversion Funnel for: {media_name} (Lifetime Totals)"
    )

    fig.update_layout(
        template="plotly_white",
        showlegend=False,
        height=300
    )

    fig.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",     # numbers on the right end of bars
        marker_color=["#1f77b4", "#ff7f0e"],
        marker_line_width=0
    )

    return fig
