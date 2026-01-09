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


def is_logged_in():
    """Check if user is logged in"""
    return 'username' in session and 'password' in session


def get_auth():
    """Get authentication from session"""
    if is_logged_in():
        return HTTPBasicAuth(session['username'], session['password'])
    return None


def require_login(f):
    """Decorator to require login"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function


@malaria_bp.route('/')
def malaria_dashboard():
    """Render malaria endemic channel dashboard"""
    if not is_logged_in():
        from flask import redirect, url_for
        return redirect(url_for('login'))
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
        
        # Sort all data by epi_week for proper chart display
        channel_df = channel_df.sort_values('epi_week')
        current_df = current_df.sort_values('epi_week')
        analysis_df = analysis_df.sort_values('epi_week')
        
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
                'baseline_years': sorted([int(y) for y in baseline_df['year'].unique()]),
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


@malaria_bp.route('/api/search-data-elements')
@require_login
def search_data_elements():
    """Search for malaria data elements to find the correct ID"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        query = request.args.get('query', 'malaria')
        
        # Search data elements
        response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'fields': 'id,code,displayName,shortName',
                'filter': f'displayName:ilike:{query}',
                'paging': 'false'
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        elements = data.get('dataElements', [])
        
        # Also search by code pattern
        response2 = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'fields': 'id,code,displayName,shortName',
                'filter': f'code:ilike:033B-CD01',
                'paging': 'false'
            },
            timeout=30
        )
        
        if response2.status_code == 200:
            data2 = response2.json()
            elements2 = data2.get('dataElements', [])
            # Merge and deduplicate
            existing_ids = {e['id'] for e in elements}
            for e in elements2:
                if e['id'] not in existing_ids:
                    elements.append(e)
        
        return jsonify({'dataElements': elements})
    
    except Exception as e:
        print(f"Error searching data elements: {e}")
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/test-data-element')
@require_login  
def test_data_element():
    """Test if a data element has data for a given org unit"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        element_id = request.args.get('element_id')
        orgunit_id = request.args.get('orgunit_id')
        
        if not element_id or not orgunit_id:
            return jsonify({'error': 'element_id and orgunit_id required'}), 400
        
        # Test with last 12 weeks
        current_year = datetime.now().year
        periods = [f"{current_year}W{w:02d}" for w in range(1, 13)]
        period_str = ";".join(periods)
        
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{element_id}'),
                ('dimension', f'pe:{period_str}'),
                ('dimension', f'ou:{orgunit_id}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=60
        )
        
        result = {
            'status_code': response.status_code,
            'element_id': element_id,
            'orgunit_id': orgunit_id,
            'periods_tested': periods
        }
        
        if response.status_code == 200:
            data = response.json()
            result['has_data'] = 'rows' in data and len(data.get('rows', [])) > 0
            result['row_count'] = len(data.get('rows', []))
            if result['has_data']:
                result['sample_rows'] = data['rows'][:5]
        else:
            result['error'] = response.text[:500]
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/find-element')
@require_login
def find_element():
    """Find and return the malaria data element ID being used"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        element_id = find_malaria_data_element(auth)
        
        # Get element details
        response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements/{element_id}",
            auth=auth,
            params={'fields': 'id,code,displayName,shortName'},
            timeout=30
        )
        
        if response.status_code == 200:
            element_info = response.json()
            return jsonify({
                'found': True,
                'element': element_info
            })
        else:
            return jsonify({
                'found': False,
                'element_id': element_id,
                'error': f'Could not get details: {response.status_code}'
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/debug-baseline')
@require_login
def debug_baseline():
    """Debug endpoint to see what baseline data is available"""
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Not authenticated'}), 401
        
        orgunit_id = request.args.get('orgunit')
        if not orgunit_id:
            return jsonify({'error': 'orgunit required'}), 400
        
        current_year = datetime.now().year
        start_year = current_year - 5
        end_year = current_year - 1
        
        # Find element
        element_id = find_malaria_data_element(auth)
        
        # Try to get just 1 year of data first
        test_year = end_year
        periods = [f"{test_year}W{w:02d}" for w in range(1, 53)]
        period_str = ";".join(periods)
        
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{element_id}'),
                ('dimension', f'pe:{period_str}'),
                ('dimension', f'ou:{orgunit_id}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=60
        )
        
        result = {
            'element_id': element_id,
            'orgunit_id': orgunit_id,
            'test_year': test_year,
            'baseline_range': f'{start_year}-{end_year}',
            'api_status': response.status_code
        }
        
        if response.status_code == 200:
            data = response.json()
            result['row_count'] = len(data.get('rows', []))
            result['has_data'] = result['row_count'] > 0
            if result['has_data']:
                # Show sample data
                sample_rows = []
                for row in data['rows'][:10]:
                    sample_rows.append({
                        'period': row[1],
                        'value': row[3]
                    })
                result['sample_data'] = sample_rows
            result['metadata'] = data.get('metaData', {}).get('items', {})
        else:
            result['error'] = response.text[:1000]
        
        return jsonify(result)
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def find_malaria_data_element(auth):
    """
    Find the malaria data element ID by searching for code pattern
    """
    search_patterns = [
        '033B-CD01a',
        'CD01a',
        'Malaria (Confirmed)',
        'Malaria Confirmed Cases'
    ]
    
    for pattern in search_patterns:
        try:
            # Search by code
            response = requests.get(
                f"{DHIS2_BASE_URL}/dataElements",
                auth=auth,
                params={
                    'fields': 'id,code,displayName',
                    'filter': f'code:ilike:{pattern}',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('dataElements', [])
                if elements:
                    # Return first match
                    print(f"Found malaria data element: {elements[0]}")
                    return elements[0]['id']
            
            # Also search by name
            response = requests.get(
                f"{DHIS2_BASE_URL}/dataElements",
                auth=auth,
                params={
                    'fields': 'id,code,displayName',
                    'filter': f'displayName:ilike:{pattern}',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('dataElements', [])
                # Look for confirmed cases specifically
                for elem in elements:
                    if 'confirmed' in elem.get('displayName', '').lower():
                        print(f"Found malaria data element: {elem}")
                        return elem['id']
                if elements:
                    print(f"Found malaria data element: {elements[0]}")
                    return elements[0]['id']
                    
        except Exception as e:
            print(f"Error searching for pattern {pattern}: {e}")
            continue
    
    # Fallback to hardcoded ID
    print("Using fallback data element ID")
    return MALARIA_DATA_ELEMENT['id']


def fetch_malaria_data(auth, orgunit_id, start_year, end_year):
    """
    Fetch malaria data from DHIS2 Analytics API
    Returns DataFrame with: year, epi_week, confirmed_cases
    """
    try:
        # Find the correct data element ID
        data_element = find_malaria_data_element(auth)
        print(f"Using data element ID: {data_element}")
        
        # Build period dimension (weekly periods)
        periods = []
        for year in range(start_year, end_year + 1):
            for week in range(1, 53):
                periods.append(f"{year}W{week:02d}")
        
        period_str = ";".join(periods)
        
        print(f"Fetching data for orgunit={orgunit_id}, years={start_year}-{end_year}")
        print(f"Total periods: {len(periods)}")
        
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
        
        print(f"Analytics response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Analytics error: {response.status_code} - {response.text[:500]}")
            return pd.DataFrame()
        
        data = response.json()
        
        if 'rows' not in data or len(data['rows']) == 0:
            print(f"No rows in response. Headers: {data.get('headers', [])}")
            print(f"Metadata: {data.get('metaData', {}).get('dimensions', {})}")
            return pd.DataFrame(columns=['year', 'epi_week', 'confirmed_cases'])
        
        print(f"Found {len(data['rows'])} data rows")
        
        # Parse response
        rows = []
        for row in data['rows']:
            # row format: [dx, pe, ou, value]
            period = str(row[1])  # e.g., "2024W01"
            try:
                value = float(row[3]) if row[3] else 0.0
            except (ValueError, TypeError):
                value = 0.0
            
            try:
                year = int(period[:4])
                week = int(period[5:])
            except (ValueError, TypeError):
                continue  # Skip malformed periods
            
            rows.append({
                'year': int(year),
                'epi_week': int(week),
                'confirmed_cases': float(value)
            })
        
        df = pd.DataFrame(rows)
        
        # Ensure proper types
        if not df.empty:
            df['year'] = df['year'].astype(int)
            df['epi_week'] = df['epi_week'].astype(int)
            df['confirmed_cases'] = df['confirmed_cases'].astype(float)
        
        print(f"Created DataFrame with {len(df)} rows")
        return df
    
    except Exception as e:
        print(f"Error fetching malaria data: {e}")
        traceback.print_exc()
        return pd.DataFrame()
