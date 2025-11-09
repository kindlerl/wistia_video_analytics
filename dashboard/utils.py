# -----------------------------------------------------------
# utils.py
# -----------------------------------------------------------
# Purpose:
#   Helper and formatting utilities for the Wistia Dashboard
# -----------------------------------------------------------

import pandas as pd
import numpy as np

def percent_format(value, decimals=1):
    """
    Format numeric value as a percentage string.
    Example: 0.1578 -> '15.8%'
    """
    if pd.isna(value):
        return "-"
    return f"{value:.{decimals}f}%"


def trend_indicator(current, previous):
    """
    Return an emoji indicator showing trend direction.
    ↑ increase, ↓ decrease, → no change
    """
    if pd.isna(current) or pd.isna(previous):
        return ""
    if current > previous:
        return "📈"
    elif current < previous:
        return "📉"
    else:
        return "→"


def safe_numeric(df, cols):
    """
    Ensure columns in 'cols' are numeric, coercing errors to NaN.
    """
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def summarize_kpis(df):
    """
    Compute aggregate KPIs from the filtered dataframe.
    Returns a dictionary with formatted results.
    """
    if df.empty:
        return {
            "total_plays": "0",
            "total_visitors": "0",
            "avg_play_rate": "-",
            "avg_engagement": "-"
        }

    return {
        "total_plays": f"{int(df['total_plays'].sum()):,}",
        "total_visitors": f"{int(df['total_visitors'].sum()):,}",
        "avg_play_rate": f"{df['avg_play_rate'].mean():.1f}",
        "avg_engagement": f"{df['avg_engagement'].mean():.1f}"
    }


def convert_date_columns(df, date_cols):
    """
    Convert specified columns to pandas datetime format.
    """
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
