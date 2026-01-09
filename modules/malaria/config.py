"""
Configuration for Malaria Endemic Channel
Thresholds, colors, and visualization settings
"""

# DHIS2 Data Element
MALARIA_DATA_ELEMENT = {
    'id': 'tE0G64cijAT',  # 033B-CD01a. Malaria (Confirmed) - Cases
    'code': '033B-CD01a',
    'name': 'Malaria (Confirmed) - Cases'
}

# Baseline period configuration
BASELINE_YEARS = 5  # Number of years for baseline calculation
EPIDEMIOLOGICAL_WEEKS = 52  # Standard epi weeks per year

# Percentile thresholds
PERCENTILES = {
    'q1': 25,      # Lower bound (success zone)
    'median': 50,  # Expected/typical cases
    'q3': 75,      # Primary epidemic threshold (WHO standard)
    'q85': 85      # Alternative threshold (reduces false alarms)
}

# Alert zones configuration
ALERT_ZONES = {
    'epidemic': {
        'name': 'Epidemic Alert',
        'color': '#e74c3c',      # Red
        'fill_color': 'rgba(231, 76, 60, 0.2)',
        'threshold': 'q3',
        'description': 'Cases above 75th percentile - Immediate action required'
    },
    'alert': {
        'name': 'Alert Zone',
        'color': '#f39c12',      # Orange
        'fill_color': 'rgba(243, 156, 18, 0.2)',
        'description': 'Cases between median and 75th percentile - Monitor closely'
    },
    'safety': {
        'name': 'Safety Zone',
        'color': '#27ae60',      # Green
        'fill_color': 'rgba(39, 174, 96, 0.2)',
        'description': 'Cases between 25th percentile and median - Normal range'
    },
    'success': {
        'name': 'Success Zone',
        'color': '#2ecc71',      # Light green
        'fill_color': 'rgba(46, 204, 113, 0.15)',
        'description': 'Cases below 25th percentile - Excellent control'
    }
}

# Visualization colors
COLORS = {
    'median_line': '#3498db',       # Blue
    'current_year': '#2c3e50',      # Dark gray/blue
    'q1_line': '#95a5a6',           # Light gray
    'q3_line': '#e67e22',           # Orange
    'channel_fill': 'rgba(52, 152, 219, 0.1)',  # Light blue
    'alert_marker': '#e74c3c',      # Red
    'background': '#ffffff',
    'grid': '#ecf0f1'
}

# Chart settings
CHART_CONFIG = {
    'width': 1200,
    'height': 600,
    'title_font_size': 18,
    'axis_font_size': 12,
    'legend_font_size': 11,
    'show_grid': True,
    'line_width': 2,
    'marker_size': 8
}

# Alert detection settings
ALERT_SETTINGS = {
    'default_threshold': 'q3',      # Use 75th percentile by default
    'consecutive_weeks': 1,         # Number of consecutive weeks to confirm alert (1 = immediate)
    'apply_consecutive_rule': False  # Toggle for 2-week rule
}

# Data processing
DATA_CONFIG = {
    'interpolate_missing': True,    # Interpolate missing weeks
    'max_missing_weeks': 4,         # Max consecutive missing weeks to interpolate
    'min_baseline_weeks': 40        # Minimum weeks required per baseline year (out of 52)
}
