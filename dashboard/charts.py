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


# def engagement_playrate_trend_chart(df: pd.DataFrame):
#     """
#     Line chart showing average engagement and play rate over time.
#     """
#     df_sorted = df.sort_values("load_date")

#     fig = px.line(
#         df_sorted,
#         x="load_date",
#         y=["avg_engagement", "avg_play_rate"],
#         color_discrete_sequence=["#FF7F0E", "#1F77B4"],
#         markers=True,
#         title="Engagement vs Play Rate Trends Over Time",
#         labels={
#             "load_date": "Load Date",
#             "value": "Percentage",
#             "variable": "Metric",
#         },
#     )
#     fig.update_layout(
#         xaxis_title="Load Date",
#         yaxis_title="Percentage",
#         legend_title="Metric",
#         template="plotly_white",
#     )
#     return fig
def engagement_playrate_trend_chart(df, highlight_date=None):
    df_sorted = df.sort_values("load_date")
    fig = px.line(
        df_sorted,
        x="load_date",
        y=["avg_engagement", "avg_play_rate"],
        color_discrete_sequence=["#FF7F0E", "#1F77B4"],
        markers=True,
        title="Engagement vs Play Rate Trends Over Time",
        labels={
            "load_date": "Load Date",
            "value": "Percentage",
            "variable": "Metric",
        },
    )

    if highlight_date:
        fig.add_vline(
            x=highlight_date,
            line_width=2,
            line_dash="dot",
            line_color="green",
            annotation_text="Selected Date",
            annotation_position="top right"
        )

    fig.update_layout(template="plotly_white")
    return fig

