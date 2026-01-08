"""
HMIS 033b Weekly Reporting Rates Module
Monitors weekly epidemiological surveillance reporting compliance
"""
from flask import Blueprint, request, jsonify, render_template, session, redirect, url_for
import requests
from requests.auth import HTTPBasicAuth
from functools import wraps
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Import from core module
try:
    from modules.core import (
        is_logged_in, get_auth, analytics_cache, http_session,
        DHIS2_BASE_URL
    )
except ImportError:
    DHIS2_BASE_URL = "https://hmis.health.go.ug/api"
    http_session = requests.Session()
    
    def is_logged_in():
        return 'username' in session and 'password' in session
    
    def get_auth():
        if 'username' in session and 'password' in session:
            return HTTPBasicAuth(session['username'], session['password'])
        return None
    
    class SimpleCache:
        def __init__(self):
            self.cache = {}
        def get(self, key):
            return self.cache.get(key)
        def set(self, key, value, ttl=300):
            self.cache[key] = value
    
    analytics_cache = SimpleCache()

# Create Blueprint
reporting_bp = Blueprint('reporting', __name__, url_prefix='/reporting')

# 033b Reporting Rate Indicators
# Note: For DHIS2, reporting rates use dataSet ID with .REPORTING_RATE suffix
REPORTING_INDICATORS = {
    'reportingRate': {
        'id': 'C4oUitImBPK.REPORTING_RATE',  # DataSet ID with reporting rate suffix
        'name': 'HMIS 033b - Reporting Rate',
        'description': 'Percentage of expected reports submitted'
    }
}

def get_color_for_rate(rate):
    """Color coding for reporting rates"""
    if rate >= 90:
        return 'green'
    elif rate >= 70:
        return 'yellow'
    else:
        return 'red'


@reporting_bp.route('/')
def dashboard():
    """033b Weekly Reporting Dashboard page"""
    if not is_logged_in():
        return redirect(url_for('login'))
    return render_template('reporting.html')


@reporting_bp.route('/api/reporting-data')
def get_reporting_data():
    """
    Fetch 033b weekly reporting rates from DHIS2
    Data is already calculated - just display as percentages
    """
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit')
    period = request.args.get('period', 'LAST_12_WEEKS')  # Weekly periods
    
    if not org_unit:
        return jsonify({'error': 'Organization unit is required'})
    
    # Check cache
    cache_key = f"reporting_{org_unit}_{period}"
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    try:
        # Get organization unit details
        org_response = http_session.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{org_unit}",
            auth=auth,
            params={'fields': 'id,displayName,level'},
            timeout=30
        )
        
        if org_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch organization unit details'})
        
        org_data = org_response.json()
        org_name = org_data.get('displayName', '')
        
        # Fetch the indicator
        indicator_id = REPORTING_INDICATORS['reportingRate']['id']
        
        dx_dimension = indicator_id
        
        # Fetch analytics data
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{period}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        analytics_response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics.json",
            auth=auth,
            params=params,
            timeout=60
        )
        
        if analytics_response.status_code != 200:
            return jsonify({
                'error': f'Analytics API error: {analytics_response.status_code}',
                'details': analytics_response.text[:200]
            })
        
        analytics_data = analytics_response.json()
        rows = analytics_data.get('rows', [])
        headers = analytics_data.get('headers', [])
        
        # Find column indices
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), len(headers) - 1)
        
        # Organize data by period and indicator
        weekly_data = {}
        
        for row in rows:
            if len(row) > max(dx_idx, pe_idx, val_idx):
                dx_id = row[dx_idx]
                period_id = row[pe_idx]
                try:
                    value = float(row[val_idx])
                except (ValueError, TypeError):
                    value = 0
                
                if period_id not in weekly_data:
                    weekly_data[period_id] = {
                        'period': period_id,
                        'reportingRate': 0
                    }
                
                if dx_id == REPORTING_INDICATORS['reportingRate']['id']:
                    weekly_data[period_id]['reportingRate'] = round(value, 1)
        
        # Convert to list and sort by period (most recent first)
        weekly_list = list(weekly_data.values())
        weekly_list.sort(key=lambda x: x['period'], reverse=True)
        
        # Add color coding
        for week in weekly_list:
            week['reportingRateColor'] = get_color_for_rate(week['reportingRate'])
        
        # Calculate summary statistics
        if weekly_list:
            avg_reporting = sum(w['reportingRate'] for w in weekly_list) / len(weekly_list)
        else:
            avg_reporting = 0
        
        result = {
            'orgUnit': org_name,
            'period': period,
            'weeklyData': weekly_list,
            'summary': {
                'avgReportingRate': round(avg_reporting, 1),
                'totalWeeks': len(weekly_list),
                'weeksAbove90': sum(1 for w in weekly_list if w['reportingRate'] >= 90),
                'weeksBelow70': sum(1 for w in weekly_list if w['reportingRate'] < 70)
            },
            '_cached': False
        }
        
        analytics_cache.set(cache_key, result, ttl=300)
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try a smaller time period'})
    except Exception as e:
        logger.error(f"Error in get_reporting_data: {e}")
        return jsonify({'error': str(e)})

