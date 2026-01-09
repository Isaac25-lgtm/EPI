"""
Flask routes for Malaria Endemic Channel module
Uses session-based authentication like other modules
"""

from flask import render_template, request, jsonify, session
from requests.auth import HTTPBasicAuth
from datetime import datetime
import traceback
import requests
import pandas as pd
import numpy as np

from modules.malaria import malaria_bp
from modules.malaria.channel_calculator import EndemicChannelCalculator
from modules.malaria.config import MALARIA_DATA_ELEMENT, BASELINE_YEARS

# Use the same DHIS2 URL as main app
DHIS2_BASE_URL = 'https://hmis.health.go.ug/api'


def get_auth():
    """Get authentication from session"""
    username = session.get('username')
    password = session.get('password')
    if not username or not password:
        return None
    return HTTPBasicAuth(username, password)


def require_login(f):
    """Decorator to require login"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function


@malaria_bp.route('/')
def malaria_dashboard():
    """Render malaria endemic channel dashboard"""
    if not session.get('logged_in'):
        from flask import redirect
        return redirect('/')
    return render_template('malaria.html')


@malaria_bp.route('/api/channel-data')
@require_login
def get_channel_data():
    """
    API endpoint to get endemic channel data
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        # Get parameters
        orgunit_id = request.args.get('orgunit')
        current_year = int(request.args.get('year', datetime.now().year))
        threshold = request.args.get('threshold', 'q3')
        
        if not orgunit_id:
            return jsonify({'error': 'Organization unit ID required'}), 400
        
        # Get baseline data (5 years)
        start_year = current_year - BASELINE_YEARS
        end_year = current_year - 1
        
        baseline_df = fetch_malaria_data(auth, orgunit_id, start_year, end_year)
        
        if baseline_df.empty:
            return jsonify({'error': 'No baseline data available'}), 404
        
        # Get current year data
        current_df = fetch_malaria_data(auth, orgunit_id, current_year, current_year)
        
        if current_df.empty:
            return jsonify({'error': 'No current year data available'}), 404
        
        # Calculate endemic channel
        calculator = EndemicChannelCalculator(threshold_percentile=threshold)
        channel_df = calculator.calculate_channel(baseline_df)
        
        # Detect alerts
        analysis_df = calculator.detect_alerts(current_df, channel_df)
        
        # Calculate z-scores
        analysis_df = calculator.calculate_z_scores(analysis_df)
        
        # Get summaries
        alert_summary = calculator.get_alert_summary(analysis_df)
        zone_distribution = calculator.get_zone_distribution(analysis_df)
        year_comparisons = calculator.compare_years(baseline_df, current_df, channel_df)
        trend = calculator.get_trend_indicator(analysis_df)
        
        # Prepare response
        response_data = {
            'channel': channel_df.to_dict('records'),
            'current_data': current_df[['epi_week', 'confirmed_cases']].to_dict('records'),
            'analysis': analysis_df[[
                'epi_week', 'confirmed_cases', 'q1', 'median', 'q3', 'q85',
                'is_alert', 'is_confirmed_alert', 'alert_zone', 'alert_status',
                'deviation_percent', 'z_score'
            ]].to_dict('records'),
            'alert_summary': alert_summary,
            'zone_distribution': zone_distribution,
            'year_comparisons': year_comparisons,
            'trend': trend,
            'metadata': {
                'orgunit_id': orgunit_id,
                'current_year': current_year,
                'threshold': threshold,
                'baseline_years': sorted(baseline_df['year'].unique().tolist()),
                'data_element': MALARIA_DATA_ELEMENT['id']
            }
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        print(f"Error in get_channel_data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/export-data')
@require_login
def export_channel_data():
    """Export channel data as CSV"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        orgunit_id = request.args.get('orgunit')
        current_year = int(request.args.get('year', datetime.now().year))
        threshold = request.args.get('threshold', 'q3')
        
        if not orgunit_id:
            return jsonify({'error': 'Organization unit ID required'}), 400
        
        # Fetch data
        start_year = current_year - BASELINE_YEARS
        end_year = current_year - 1
        baseline_df = fetch_malaria_data(auth, orgunit_id, start_year, end_year)
        current_df = fetch_malaria_data(auth, orgunit_id, current_year, current_year)
        
        # Calculate
        calculator = EndemicChannelCalculator(threshold_percentile=threshold)
        channel_df = calculator.calculate_channel(baseline_df)
        analysis_df = calculator.detect_alerts(current_df, channel_df)
        
        # Prepare export
        export_df = analysis_df[[
            'epi_week', 'confirmed_cases', 'q1', 'median', 'q3', 'q85',
            'is_alert', 'alert_zone', 'alert_status', 'deviation_percent'
        ]].copy()
        
        export_df.columns = [
            'Epi Week', 'Cases', 'Q1 (25th)', 'Median (50th)', 'Q3 (75th)', 'Q85 (85th)',
            'Is Alert', 'Alert Zone', 'Status', 'Deviation %'
        ]
        
        csv_data = export_df.to_csv(index=False)
        
        from flask import Response
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=malaria_channel_{orgunit_id}_{current_year}.csv'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/orgunit-search')
@require_login
def search_orgunits():
    """Search for organization units"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        query = request.args.get('query', '')
        
        if len(query) < 2:
            return jsonify({'orgunits': []})
        
        # Search org units
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits",
            auth=auth,
            params={
                'fields': 'id,displayName~rename(name),level',
                'filter': f'displayName:ilike:{query}',
                'paging': 'false'
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        orgunits = data.get('organisationUnits', [])
        
        return jsonify({'orgunits': orgunits})
    
    except Exception as e:
        print(f"Error in search_orgunits: {e}")
        return jsonify({'error': str(e)}), 500


def fetch_malaria_data(auth, orgunit_id, start_year, end_year):
    """
    Fetch malaria data from DHIS2 Analytics API
    Returns DataFrame with: year, epi_week, confirmed_cases
    """
    try:
        # Build period dimension (weekly periods)
        periods = []
        for year in range(start_year, end_year + 1):
            for week in range(1, 53):
                periods.append(f"{year}W{week:02d}")
        
        period_str = ";".join(periods)
        data_element = MALARIA_DATA_ELEMENT['id']
        
        # Call analytics API
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{data_element}'),
                ('dimension', f'pe:{period_str}'),
                ('dimension', f'ou:{orgunit_id}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=120
        )
        
        if response.status_code != 200:
            print(f"Analytics error: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        if 'rows' not in data or len(data['rows']) == 0:
            return pd.DataFrame(columns=['year', 'epi_week', 'confirmed_cases'])
        
        # Parse response
        rows = []
        for row in data['rows']:
            # row format: [dx, pe, ou, value]
            period = row[1]  # e.g., "2024W01"
            value = float(row[3]) if row[3] else 0
            
            year = int(period[:4])
            week = int(period[5:])
            
            rows.append({
                'year': year,
                'epi_week': week,
                'confirmed_cases': value
            })
        
        df = pd.DataFrame(rows)
        return df
    
    except Exception as e:
        print(f"Error fetching malaria data: {e}")
        traceback.print_exc()
        return pd.DataFrame()
