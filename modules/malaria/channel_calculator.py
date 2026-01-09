"""
Endemic Channel Calculator
Implements WHO percentile method for malaria epidemic detection
"""

import pandas as pd
import numpy as np

from modules.malaria.config import PERCENTILES, ALERT_SETTINGS
from modules.malaria.utils import (
    get_alert_status, get_alert_color, 
    detect_consecutive_alerts, calculate_summary_stats
)


class EndemicChannelCalculator:
    """Calculate endemic channel thresholds and detect alerts"""
    
    def __init__(self, threshold_percentile='q3', apply_consecutive_rule=False, consecutive_weeks=2):
        """
        Initialize calculator
        Args:
            threshold_percentile: Percentile to use for epidemic threshold ('q3' or 'q85')
            apply_consecutive_rule: Whether to require consecutive weeks for alert
            consecutive_weeks: Number of consecutive weeks required
        """
        self.threshold_percentile = threshold_percentile
        self.apply_consecutive_rule = apply_consecutive_rule
        self.consecutive_weeks = consecutive_weeks
    
    def calculate_channel(self, baseline_df):
        """
        Calculate endemic channel thresholds from baseline data
        Args:
            baseline_df: DataFrame with columns [year, epi_week, confirmed_cases]
        Returns:
            DataFrame with columns [epi_week, q1, median, q3, q85, mean, std]
        """
        # Group by epidemiological week
        channel_data = []
        
        for week in range(1, 53):
            week_data = baseline_df[baseline_df['epi_week'] == week]['confirmed_cases']
            
            if len(week_data) == 0:
                # No data for this week - use overall baseline statistics
                week_data = baseline_df['confirmed_cases']
            
            # Calculate percentiles
            q1 = np.percentile(week_data, PERCENTILES['q1'])
            median = np.percentile(week_data, PERCENTILES['median'])
            q3 = np.percentile(week_data, PERCENTILES['q3'])
            q85 = np.percentile(week_data, PERCENTILES['q85'])
            mean = np.mean(week_data)
            std = np.std(week_data)
            
            channel_data.append({
                'epi_week': week,
                'q1': round(q1, 1),
                'median': round(median, 1),
                'q3': round(q3, 1),
                'q85': round(q85, 1),
                'mean': round(mean, 1),
                'std': round(std, 1),
                'n_observations': len(week_data)
            })
        
        channel_df = pd.DataFrame(channel_data)
        return channel_df
    
    def detect_alerts(self, current_df, channel_df):
        """
        Detect epidemic alerts by comparing current data to channel thresholds
        Args:
            current_df: DataFrame with current year data [epi_week, confirmed_cases]
            channel_df: DataFrame with channel thresholds
        Returns:
            DataFrame with alert detection results
        """
        # Merge current data with thresholds
        analysis_df = current_df.merge(channel_df, on='epi_week', how='left')
        
        # Determine threshold column based on settings
        threshold_col = self.threshold_percentile
        
        # Detect alerts
        analysis_df['is_alert'] = analysis_df['confirmed_cases'] > analysis_df[threshold_col]
        analysis_df['threshold_value'] = analysis_df[threshold_col]
        
        # Calculate deviations
        analysis_df['deviation_from_median'] = analysis_df['confirmed_cases'] - analysis_df['median']
        analysis_df['deviation_percent'] = (
            (analysis_df['confirmed_cases'] - analysis_df['median']) / 
            analysis_df['median'].replace(0, np.nan)
        ) * 100
        analysis_df['deviation_percent'] = analysis_df['deviation_percent'].round(1)
        
        # Assign alert zones
        analysis_df['alert_zone'] = analysis_df.apply(
            lambda row: self._assign_alert_zone(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            ), 
            axis=1
        )
        
        analysis_df['alert_color'] = analysis_df.apply(
            lambda row: get_alert_color(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            ), 
            axis=1
        )
        
        analysis_df['alert_status'] = analysis_df.apply(
            lambda row: get_alert_status(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            ), 
            axis=1
        )
        
        # Apply consecutive weeks rule if enabled
        if self.apply_consecutive_rule:
            alert_weeks = analysis_df[analysis_df['is_alert']]['epi_week'].tolist()
            confirmed_alert_weeks = detect_consecutive_alerts(
                alert_weeks, 
                self.consecutive_weeks
            )
            analysis_df['is_confirmed_alert'] = analysis_df['epi_week'].isin(confirmed_alert_weeks)
        else:
            analysis_df['is_confirmed_alert'] = analysis_df['is_alert']
        
        return analysis_df
    
    def _assign_alert_zone(self, current, q1, median, q3):
        """Assign alert zone based on current value"""
        if current > q3:
            return 'epidemic'
        elif current > median:
            return 'alert'
        elif current > q1:
            return 'safety'
        else:
            return 'success'
    
    def get_alert_summary(self, analysis_df):
        """
        Get summary of alerts
        Returns:
            Dictionary with alert summary statistics
        """
        alert_df = analysis_df[analysis_df['is_confirmed_alert']].copy()
        
        if len(alert_df) == 0:
            return {
                'total_alert_weeks': 0,
                'alert_weeks': [],
                'max_cases_week': None,
                'max_cases': 0,
                'avg_deviation': 0,
                'total_excess_cases': 0
            }
        
        # Sort by cases descending
        alert_df = alert_df.sort_values('confirmed_cases', ascending=False)
        
        summary = {
            'total_alert_weeks': len(alert_df),
            'alert_weeks': alert_df['epi_week'].tolist(),
            'max_cases_week': int(alert_df.iloc[0]['epi_week']),
            'max_cases': int(alert_df.iloc[0]['confirmed_cases']),
            'avg_deviation': round(alert_df['deviation_percent'].mean(), 1),
            'total_excess_cases': int(alert_df['deviation_from_median'].sum())
        }
        
        return summary
    
    def get_zone_distribution(self, analysis_df):
        """
        Get distribution of weeks across alert zones
        Returns:
            Dictionary with zone counts and percentages
        """
        total_weeks = len(analysis_df)
        
        zone_counts = analysis_df['alert_zone'].value_counts().to_dict()
        
        distribution = {}
        for zone in ['success', 'safety', 'alert', 'epidemic']:
            count = zone_counts.get(zone, 0)
            distribution[zone] = {
                'count': count,
                'percentage': round((count / total_weeks) * 100, 1) if total_weeks > 0 else 0
            }
        
        return distribution
    
    def compare_years(self, baseline_df, current_df, channel_df):
        """
        Compare current year against individual baseline years
        Returns:
            Dictionary with comparative statistics
        """
        baseline_years = sorted(baseline_df['year'].unique())
        
        comparisons = {}
        
        for year in baseline_years:
            year_df = baseline_df[baseline_df['year'] == year]
            year_total = year_df['confirmed_cases'].sum()
            comparisons[year] = {
                'total_cases': int(year_total),
                'avg_weekly': round(year_df['confirmed_cases'].mean(), 1)
            }
        
        # Current year
        current_total = current_df['confirmed_cases'].sum()
        current_weeks = len(current_df)
        
        comparisons['current'] = {
            'total_cases': int(current_total),
            'avg_weekly': round(current_df['confirmed_cases'].mean(), 1),
            'weeks_reported': current_weeks
        }
        
        # Calculate 5-year baseline average
        baseline_total = baseline_df['confirmed_cases'].sum()
        baseline_weeks = len(baseline_df)
        
        comparisons['baseline_avg'] = {
            'total_cases': int(baseline_total / len(baseline_years)),
            'avg_weekly': round(baseline_df['confirmed_cases'].mean(), 1)
        }
        
        # Compare current to baseline
        if comparisons['baseline_avg']['total_cases'] > 0:
            percent_change = (
                (comparisons['current']['total_cases'] - comparisons['baseline_avg']['total_cases']) /
                comparisons['baseline_avg']['total_cases']
            ) * 100
            comparisons['current']['percent_vs_baseline'] = round(percent_change, 1)
        else:
            comparisons['current']['percent_vs_baseline'] = 0
        
        return comparisons
    
    def calculate_z_scores(self, analysis_df):
        """
        Calculate z-scores for statistical analysis
        Args:
            analysis_df: DataFrame with current data and channel thresholds
        Returns:
            DataFrame with z-scores added
        """
        analysis_df['z_score'] = (
            (analysis_df['confirmed_cases'] - analysis_df['mean']) / 
            analysis_df['std'].replace(0, np.nan)
        ).round(2)
        
        # Flag statistical outliers (|z| > 2)
        analysis_df['is_statistical_outlier'] = analysis_df['z_score'].abs() > 2
        
        return analysis_df
    
    def get_trend_indicator(self, analysis_df, window=4):
        """
        Calculate trend indicator (increasing/decreasing) using moving average
        Args:
            analysis_df: DataFrame with current data
            window: Number of weeks for moving average
        Returns:
            String: 'increasing', 'decreasing', or 'stable'
        """
        if len(analysis_df) < window + 1:
            return 'insufficient_data'
        
        recent_data = analysis_df.tail(window + 1)
        
        # Calculate moving average slope
        recent_data['ma'] = recent_data['confirmed_cases'].rolling(window=window).mean()
        
        if recent_data['ma'].iloc[-1] > recent_data['ma'].iloc[-2] * 1.1:
            return 'increasing'
        elif recent_data['ma'].iloc[-1] < recent_data['ma'].iloc[-2] * 0.9:
            return 'decreasing'
        else:
            return 'stable'
