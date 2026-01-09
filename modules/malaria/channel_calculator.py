"""
Endemic Channel Calculator
Implements WHO percentile method for malaria epidemic detection
"""

import pandas as pd
import numpy as np

from modules.malaria.config import PERCENTILES, ALERT_SETTINGS
from modules.malaria.utils import (
    safe_float, safe_int,
    get_alert_status, get_alert_color, 
    detect_consecutive_alerts
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
    
    def _ensure_numeric_df(self, df):
        """Ensure DataFrame has proper numeric types"""
        df = df.copy()
        
        if 'confirmed_cases' in df.columns:
            df['confirmed_cases'] = df['confirmed_cases'].apply(safe_float)
        if 'epi_week' in df.columns:
            df['epi_week'] = df['epi_week'].apply(safe_int)
        if 'year' in df.columns:
            df['year'] = df['year'].apply(safe_int)
        
        for col in ['q1', 'median', 'q3', 'q85', 'mean', 'std']:
            if col in df.columns:
                df[col] = df[col].apply(safe_float)
        
        return df
    
    def calculate_channel(self, baseline_df):
        """
        Calculate endemic channel thresholds from baseline data
        Args:
            baseline_df: DataFrame with columns [year, epi_week, confirmed_cases]
        Returns:
            DataFrame with columns [epi_week, q1, median, q3, q85, mean, std]
        """
        # Ensure numeric types
        baseline_df = self._ensure_numeric_df(baseline_df)
        
        # Group by epidemiological week
        channel_data = []
        
        for week in range(1, 53):
            week_mask = baseline_df['epi_week'] == week
            week_values = baseline_df.loc[week_mask, 'confirmed_cases'].tolist()
            
            if len(week_values) == 0:
                # No data for this week - use overall baseline statistics
                week_values = baseline_df['confirmed_cases'].tolist()
            
            # Convert to numpy array of floats
            week_array = np.array([safe_float(v) for v in week_values], dtype=np.float64)
            
            if len(week_array) == 0:
                q1 = median = q3 = q85 = mean = std = 0.0
            else:
                q1 = float(np.percentile(week_array, PERCENTILES['q1']))
                median = float(np.percentile(week_array, PERCENTILES['median']))
                q3 = float(np.percentile(week_array, PERCENTILES['q3']))
                q85 = float(np.percentile(week_array, PERCENTILES['q85']))
                mean = float(np.mean(week_array))
                std = float(np.std(week_array))
            
            channel_data.append({
                'epi_week': week,
                'q1': round(q1, 1),
                'median': round(median, 1),
                'q3': round(q3, 1),
                'q85': round(q85, 1),
                'mean': round(mean, 1),
                'std': round(std, 1),
                'n_observations': len(week_array)
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
        # Ensure numeric types
        current_df = self._ensure_numeric_df(current_df)
        channel_df = self._ensure_numeric_df(channel_df)
        
        # Merge current data with thresholds
        analysis_df = current_df.merge(channel_df, on='epi_week', how='left')
        
        # Ensure all numeric columns are float after merge
        analysis_df = self._ensure_numeric_df(analysis_df)
        
        # Determine threshold column based on settings
        threshold_col = self.threshold_percentile
        
        # Detect alerts using explicit float comparison
        def check_alert(row):
            cases = safe_float(row['confirmed_cases'])
            threshold = safe_float(row[threshold_col])
            return cases > threshold
        
        analysis_df['is_alert'] = analysis_df.apply(check_alert, axis=1)
        analysis_df['threshold_value'] = analysis_df[threshold_col].apply(safe_float)
        
        # Calculate deviations
        def calc_deviation(row):
            cases = safe_float(row['confirmed_cases'])
            median = safe_float(row['median'])
            return cases - median
        
        analysis_df['deviation_from_median'] = analysis_df.apply(calc_deviation, axis=1)
        
        def calc_deviation_percent(row):
            cases = safe_float(row['confirmed_cases'])
            median = safe_float(row['median'])
            if median == 0:
                return 0.0
            return round(((cases - median) / median) * 100, 1)
        
        analysis_df['deviation_percent'] = analysis_df.apply(calc_deviation_percent, axis=1)
        
        # Assign alert zones
        def assign_zone(row):
            return self._assign_alert_zone(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            )
        
        analysis_df['alert_zone'] = analysis_df.apply(assign_zone, axis=1)
        
        def assign_color(row):
            return get_alert_color(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            )
        
        analysis_df['alert_color'] = analysis_df.apply(assign_color, axis=1)
        
        def assign_status(row):
            return get_alert_status(
                row['confirmed_cases'], 
                row['q1'], 
                row['median'], 
                row['q3']
            )
        
        analysis_df['alert_status'] = analysis_df.apply(assign_status, axis=1)
        
        # Apply consecutive weeks rule if enabled
        if self.apply_consecutive_rule:
            alert_weeks = [safe_int(w) for w in analysis_df[analysis_df['is_alert'] == True]['epi_week'].tolist()]
            confirmed_alert_weeks = detect_consecutive_alerts(
                alert_weeks, 
                self.consecutive_weeks
            )
            analysis_df['is_confirmed_alert'] = analysis_df['epi_week'].apply(
                lambda x: safe_int(x) in confirmed_alert_weeks
            )
        else:
            analysis_df['is_confirmed_alert'] = analysis_df['is_alert']
        
        return analysis_df
    
    def _assign_alert_zone(self, current, q1, median, q3):
        """Assign alert zone based on current value"""
        current = safe_float(current)
        q1 = safe_float(q1)
        median = safe_float(median)
        q3 = safe_float(q3)
        
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
        alert_df = analysis_df[analysis_df['is_confirmed_alert'] == True].copy()
        
        if len(alert_df) == 0:
            return {
                'total_alert_weeks': 0,
                'alert_weeks': [],
                'max_cases_week': None,
                'max_cases': 0,
                'avg_deviation': 0.0,
                'total_excess_cases': 0
            }
        
        # Sort by cases descending - ensure numeric
        alert_df['_sort_cases'] = alert_df['confirmed_cases'].apply(safe_float)
        alert_df = alert_df.sort_values('_sort_cases', ascending=False)
        
        summary = {
            'total_alert_weeks': len(alert_df),
            'alert_weeks': [safe_int(w) for w in alert_df['epi_week'].tolist()],
            'max_cases_week': safe_int(alert_df.iloc[0]['epi_week']),
            'max_cases': safe_int(alert_df.iloc[0]['confirmed_cases']),
            'avg_deviation': round(safe_float(alert_df['deviation_percent'].mean()), 1),
            'total_excess_cases': safe_int(alert_df['deviation_from_median'].sum())
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
                'count': int(count),
                'percentage': round((count / total_weeks) * 100, 1) if total_weeks > 0 else 0.0
            }
        
        return distribution
    
    def compare_years(self, baseline_df, current_df, channel_df):
        """
        Compare current year against individual baseline years
        Returns:
            Dictionary with comparative statistics
        """
        # Ensure numeric types
        baseline_df = self._ensure_numeric_df(baseline_df)
        current_df = self._ensure_numeric_df(current_df)
        
        baseline_years = sorted([safe_int(y) for y in baseline_df['year'].unique()])
        
        comparisons = {}
        
        for year in baseline_years:
            year_df = baseline_df[baseline_df['year'] == year]
            year_total = sum([safe_float(v) for v in year_df['confirmed_cases']])
            year_mean = year_total / len(year_df) if len(year_df) > 0 else 0.0
            # Use string keys to avoid mixed type sorting in JSON
            comparisons[str(year)] = {
                'total_cases': int(year_total),
                'avg_weekly': round(year_mean, 1)
            }
        
        # Current year
        current_total = sum([safe_float(v) for v in current_df['confirmed_cases']])
        current_weeks = len(current_df)
        current_mean = current_total / current_weeks if current_weeks > 0 else 0.0
        
        comparisons['current'] = {
            'total_cases': int(current_total),
            'avg_weekly': round(current_mean, 1),
            'weeks_reported': current_weeks
        }
        
        # Calculate 5-year baseline average
        baseline_total = sum([safe_float(v) for v in baseline_df['confirmed_cases']])
        num_years = len(baseline_years) if len(baseline_years) > 0 else 1
        baseline_mean = baseline_total / len(baseline_df) if len(baseline_df) > 0 else 0.0
        
        comparisons['baseline_avg'] = {
            'total_cases': int(baseline_total / num_years),
            'avg_weekly': round(baseline_mean, 1)
        }
        
        # Compare current to baseline
        if comparisons['baseline_avg']['total_cases'] > 0:
            percent_change = (
                (comparisons['current']['total_cases'] - comparisons['baseline_avg']['total_cases']) /
                comparisons['baseline_avg']['total_cases']
            ) * 100
            comparisons['current']['percent_vs_baseline'] = round(percent_change, 1)
        else:
            comparisons['current']['percent_vs_baseline'] = 0.0
        
        return comparisons
    
    def calculate_z_scores(self, analysis_df):
        """
        Calculate z-scores for statistical analysis
        Args:
            analysis_df: DataFrame with current data and channel thresholds
        Returns:
            DataFrame with z-scores added
        """
        def calc_zscore(row):
            cases = safe_float(row['confirmed_cases'])
            mean = safe_float(row['mean'])
            std = safe_float(row['std'])
            if std == 0:
                return 0.0
            return round((cases - mean) / std, 2)
        
        analysis_df['z_score'] = analysis_df.apply(calc_zscore, axis=1)
        
        # Flag statistical outliers (|z| > 2)
        analysis_df['is_statistical_outlier'] = analysis_df['z_score'].apply(
            lambda z: abs(safe_float(z)) > 2
        )
        
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
        
        # Ensure numeric and sort
        analysis_df = self._ensure_numeric_df(analysis_df.copy())
        analysis_df = analysis_df.sort_values('epi_week')
        
        recent_data = analysis_df.tail(window + 1)
        
        # Calculate moving average
        values = [safe_float(v) for v in recent_data['confirmed_cases'].tolist()]
        
        if len(values) < 2:
            return 'insufficient_data'
        
        # Simple comparison of recent vs previous
        recent_avg = sum(values[-window:]) / window
        prev_avg = sum(values[:-1][-window:]) / window if len(values) > window else values[0]
        
        if recent_avg > prev_avg * 1.1:
            return 'increasing'
        elif recent_avg < prev_avg * 0.9:
            return 'decreasing'
        else:
            return 'stable'
