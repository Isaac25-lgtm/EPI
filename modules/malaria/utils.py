"""
Utility functions for Malaria Endemic Channel
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def get_epi_week(date):
    """
    Convert date to epidemiological week (1-52)
    Using ISO week date system
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    return date.isocalendar()[1]


def get_epi_year(date):
    """
    Get epidemiological year (may differ from calendar year for week 1)
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    iso_calendar = date.isocalendar()
    return iso_calendar[0]


def epi_week_to_date(year, week):
    """
    Convert epidemiological week to approximate date (Monday of that week)
    """
    # January 4th is always in week 1
    jan4 = datetime(year, 1, 4)
    week_1_monday = jan4 - timedelta(days=jan4.weekday())
    target_date = week_1_monday + timedelta(weeks=week - 1)
    return target_date


def format_week_label(week):
    """
    Format week number for display
    """
    return f"Week {week:02d}"


def format_week_date(year, week):
    """
    Format week with date range for tooltips
    """
    start_date = epi_week_to_date(year, week)
    end_date = start_date + timedelta(days=6)
    return f"Week {week:02d} ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d')})"


def calculate_deviation_percentage(current, baseline):
    """
    Calculate percentage deviation from baseline
    """
    if baseline == 0:
        return 0 if current == 0 else 100
    return round(((current - baseline) / baseline) * 100, 1)


def interpolate_missing_weeks(df, max_gap=4):
    """
    Interpolate missing weeks in time series data
    Args:
        df: DataFrame with 'epi_week' and 'confirmed_cases' columns
        max_gap: Maximum number of consecutive missing weeks to interpolate
    Returns:
        DataFrame with interpolated values
    """
    # Create complete week range
    all_weeks = pd.DataFrame({'epi_week': range(1, 53)})
    
    # Merge with actual data
    df_complete = all_weeks.merge(df, on='epi_week', how='left')
    
    # Identify gaps
    df_complete['is_missing'] = df_complete['confirmed_cases'].isna()
    
    # Calculate gap sizes
    df_complete['gap_group'] = (df_complete['is_missing'] != df_complete['is_missing'].shift()).cumsum()
    gap_sizes = df_complete[df_complete['is_missing']].groupby('gap_group').size()
    
    # Only interpolate small gaps
    small_gaps = gap_sizes[gap_sizes <= max_gap].index
    df_complete['should_interpolate'] = df_complete['gap_group'].isin(small_gaps) & df_complete['is_missing']
    
    # Interpolate
    df_complete.loc[df_complete['should_interpolate'], 'confirmed_cases'] = np.nan
    df_complete['confirmed_cases'] = df_complete['confirmed_cases'].interpolate(method='linear', limit=max_gap)
    
    # Mark interpolated values
    df_complete['is_interpolated'] = df_complete['should_interpolate']
    
    return df_complete[['epi_week', 'confirmed_cases', 'is_interpolated']]


def get_alert_color(current, q1, median, q3):
    """
    Determine alert zone color based on case count
    """
    if current > q3:
        return '#e74c3c'  # Red - Epidemic
    elif current > median:
        return '#f39c12'  # Orange - Alert
    elif current > q1:
        return '#27ae60'  # Green - Safety
    else:
        return '#2ecc71'  # Light green - Success


def get_alert_status(current, q1, median, q3):
    """
    Get alert status text
    """
    if current > q3:
        return 'EPIDEMIC ALERT'
    elif current > median:
        return 'ALERT ZONE'
    elif current > q1:
        return 'NORMAL'
    else:
        return 'LOW TRANSMISSION'


def validate_baseline_data(df, min_weeks=40):
    """
    Validate that baseline data has sufficient coverage
    Returns: (is_valid, message, coverage_stats)
    """
    coverage_stats = {}
    
    for year in df['year'].unique():
        year_data = df[df['year'] == year]
        weeks_present = year_data['epi_week'].nunique()
        coverage_stats[year] = {
            'weeks_present': weeks_present,
            'coverage_percent': (weeks_present / 52) * 100,
            'is_sufficient': weeks_present >= min_weeks
        }
    
    insufficient_years = [y for y, stats in coverage_stats.items() if not stats['is_sufficient']]
    
    if insufficient_years:
        return (
            False,
            f"Insufficient data for years: {insufficient_years}. Minimum {min_weeks} weeks required.",
            coverage_stats
        )
    
    return (True, "Baseline data validation passed", coverage_stats)


def aggregate_to_district(df, orgunit_hierarchy):
    """
    Aggregate facility-level data to district level
    Args:
        df: DataFrame with facility data
        orgunit_hierarchy: Dict mapping facility IDs to district IDs
    Returns:
        Aggregated DataFrame
    """
    df['district_id'] = df['orgunit'].map(orgunit_hierarchy)
    
    aggregated = df.groupby(['year', 'epi_week', 'district_id']).agg({
        'confirmed_cases': 'sum'
    }).reset_index()
    
    return aggregated


def detect_consecutive_alerts(alert_weeks, consecutive_count=2):
    """
    Apply consecutive weeks rule for confirmed alerts
    Args:
        alert_weeks: List of week numbers with alerts
        consecutive_count: Number of consecutive weeks required
    Returns:
        List of confirmed alert weeks
    """
    if consecutive_count <= 1:
        return alert_weeks
    
    confirmed_alerts = []
    alert_weeks_sorted = sorted(alert_weeks)
    
    for i in range(len(alert_weeks_sorted) - consecutive_count + 1):
        window = alert_weeks_sorted[i:i + consecutive_count]
        # Check if weeks are consecutive
        if all(window[j+1] - window[j] == 1 for j in range(len(window) - 1)):
            confirmed_alerts.extend(window)
    
    return list(set(confirmed_alerts))


def calculate_summary_stats(current_data, baseline_stats):
    """
    Calculate summary statistics for dashboard
    """
    total_weeks = len(current_data)
    alert_weeks = current_data[current_data['is_alert']]['epi_week'].tolist()
    
    current_week = current_data['epi_week'].max()
    current_week_data = current_data[current_data['epi_week'] == current_week].iloc[0]
    
    total_cases = current_data['confirmed_cases'].sum()
    expected_cases = baseline_stats['median'].sum()
    
    summary = {
        'total_weeks_monitored': total_weeks,
        'total_alert_weeks': len(alert_weeks),
        'alert_weeks_list': alert_weeks,
        'current_week': current_week,
        'current_week_cases': int(current_week_data['confirmed_cases']),
        'current_week_status': get_alert_status(
            current_week_data['confirmed_cases'],
            current_week_data['q1'],
            current_week_data['median'],
            current_week_data['q3']
        ),
        'total_cases_ytd': int(total_cases),
        'expected_cases_ytd': int(expected_cases),
        'deviation_percent': calculate_deviation_percentage(total_cases, expected_cases),
        'alert_rate': round((len(alert_weeks) / total_weeks) * 100, 1) if total_weeks > 0 else 0
    }
    
    return summary
