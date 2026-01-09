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
from modules.malaria.utils import safe_float, safe_int
from modules.malaria.incidence_calculator import (
    calculate_incidence, calculate_quartile_classification,
    calculate_weekly_incidence, rank_orgunits_by_incidence
)

# Import UBOS Population data
# This is the official Uganda Bureau of Statistics population data
from modules.malaria.ubos_population import UBOS_POPULATION
print(f"Loaded UBOS_POPULATION with {len(UBOS_POPULATION)} districts")

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


@malaria_bp.route('/api/incidence-trend')
@require_login
def get_incidence_trend():
    """
    Get 12-week incidence trend for a selected org unit
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        orgunit_id = request.args.get('orgunit')
        if not orgunit_id:
            return jsonify({'error': 'Organization unit ID required'}), 400
        
        # Find data element
        data_element = find_malaria_data_element(auth)
        print(f"Incidence trend - Using data element: {data_element}")
        
        # Use DHIS2's built-in LAST_12_WEEKS relative period (more reliable)
        period_param = 'LAST_12_WEEKS'
        
        print(f"Incidence trend - Fetching data for orgunit: {orgunit_id}")
        
        # Fetch cases
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{data_element}'),
                ('dimension', f'pe:{period_param}'),
                ('dimension', f'ou:{orgunit_id}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=60
        )
        
        print(f"Incidence trend - DHIS2 response: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Incidence trend - Error response: {response.text[:500]}")
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        rows = data.get('rows', [])
        meta_dimensions = data.get('metaData', {}).get('dimensions', {})
        
        # Extract periods from response metadata (sorted chronologically)
        periods = sorted(meta_dimensions.get('pe', []))
        
        print(f"Incidence trend - Got {len(rows)} data rows")
        print(f"Incidence trend - Periods from response: {periods}")
        
        # Parse cases into lookup
        cases_lookup = {}
        for row in rows:
            period = row[1]
            value = safe_float(row[3])
            cases_lookup[period] = value
        
        print(f"Incidence trend - Cases by period: {cases_lookup}")
        
        # Get population (from UBOS data or fetch from org unit)
        current_year = datetime.now().year
        population = fetch_orgunit_population(auth, orgunit_id, current_year)
        print(f"Incidence trend - Population for orgunit: {population}")
        
        # Build incidence data in chronological order (using periods from response)
        incidence_data = []
        for period in periods:
            cases = cases_lookup.get(period, 0)
            incidence = calculate_incidence(cases, population)
            incidence_data.append({
                'period': period,
                'cases': int(cases) if cases else 0,
                'incidence': round(incidence, 2) if incidence else 0,
                'population': population
            })
        
        # Get org unit name
        orgunit_name = get_orgunit_name(auth, orgunit_id)
        
        print(f"Incidence trend for {orgunit_name}: {len(incidence_data)} weeks")
        
        return jsonify({
            'orgunit_id': orgunit_id,
            'orgunit_name': orgunit_name,
            'population': population,
            'periods': periods,  # Include ordered periods for reference
            'data': incidence_data
        })
    
    except Exception as e:
        print(f"Error in get_incidence_trend: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/incidence-map')
@require_login
def get_incidence_map():
    """
    Get incidence data for spatial map with quartile classification
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        level = request.args.get('level', '3')  # Default to district level
        parent_id = request.args.get('parent')  # For drill-down
        
        # Find data element
        data_element = find_malaria_data_element(auth)
        
        # Get current week
        current_year = datetime.now().year
        current_week = datetime.now().isocalendar()[1]
        current_period = f"{current_year}W{current_week:02d}"
        
        # Build org unit dimension
        if parent_id:
            # Drill-down: get children of parent
            ou_dimension = f'ou:{parent_id};LEVEL-{level}'
        else:
            # Top level: all at specified level
            ou_dimension = f'ou:LEVEL-{level}'
        
        # Fetch current week cases
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{data_element}'),
                ('dimension', f'pe:{current_period}'),
                ('dimension', ou_dimension),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=60
        )
        
        if response.status_code != 200:
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        rows = data.get('rows', [])
        meta_items = data.get('metaData', {}).get('items', {})
        
        # Parse cases by org unit
        cases_by_ou = {}
        for row in rows:
            ou_id = row[2]
            value = safe_float(row[3])
            cases_by_ou[ou_id] = value
        
        # Fetch populations for all org units
        populations = fetch_populations_for_level(auth, level, parent_id, current_year)
        
        # Calculate incidence for each org unit
        incidence_by_ou = {}
        for ou_id, cases in cases_by_ou.items():
            population = populations.get(ou_id)
            incidence = calculate_incidence(cases, population)
            incidence_by_ou[ou_id] = incidence
        
        # Add org units with no cases but with population
        for ou_id in populations.keys():
            if ou_id not in incidence_by_ou:
                incidence = calculate_incidence(0, populations[ou_id])
                incidence_by_ou[ou_id] = incidence
        
        # Calculate quartile classification
        color_mapping, thresholds = calculate_quartile_classification(incidence_by_ou)
        
        # Rank org units
        ranked_data = rank_orgunits_by_incidence(incidence_by_ou)
        
        # Prepare response
        orgunits_data = []
        for ou_id, incidence in incidence_by_ou.items():
            ou_info = meta_items.get(ou_id, {})
            rank = next((r for o, i, r in ranked_data if o == ou_id), None)
            
            orgunits_data.append({
                'id': ou_id,
                'name': ou_info.get('name', ou_id),
                'cases': cases_by_ou.get(ou_id, 0),
                'population': populations.get(ou_id),
                'incidence': incidence,
                'quartile': color_mapping[ou_id]['quartile'],
                'color': color_mapping[ou_id]['color'],
                'label': color_mapping[ou_id]['label'],
                'rank': rank
            })
        
        return jsonify({
            'period': current_period,
            'level': level,
            'parent_id': parent_id,
            'thresholds': thresholds,
            'total_orgunits': len(orgunits_data),
            'orgunits': orgunits_data
        })
    
    except Exception as e:
        print(f"Error in get_incidence_map: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/incidence-table')
@require_login
def get_incidence_table():
    """
    Get incidence table for last 12 weeks for multiple org units
    Shows raw cases when population data is not available
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        level = request.args.get('level', '3')
        limit = int(request.args.get('limit', '150'))  # Get all districts, scrollable after 20
        
        print(f"=== INCIDENCE TABLE: level={level}, limit={limit} ===")
        
        # Find data element
        data_element = find_malaria_data_element(auth)
        print(f"Using malaria data element: {data_element}")
        
        # Use DHIS2's built-in LAST_12_WEEKS relative period (more reliable)
        period_param = 'LAST_12_WEEKS'
        
        print(f"Incidence table - Using period: {period_param}")
        
        # Fetch cases for all org units
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{data_element}'),
                ('dimension', f'pe:{period_param}'),
                ('dimension', f'ou:LEVEL-{level}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false')
            ],
            timeout=120
        )
        
        print(f"Incidence table - DHIS2 response: {response.status_code}")
        
        if response.status_code != 200:
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        rows = data.get('rows', [])
        meta_items = data.get('metaData', {}).get('items', {})
        meta_dimensions = data.get('metaData', {}).get('dimensions', {})
        
        # Extract periods from response metadata (sorted chronologically)
        periods = sorted(meta_dimensions.get('pe', []))
        
        print(f"Got {len(rows)} data rows, {len(meta_items)} meta items")
        print(f"Periods from response: {periods}")
        
        if not rows:
            return jsonify({'periods': periods, 'orgunits': [], 'message': 'No data returned from DHIS2'})
        
        # Build cases by orgunit and period
        cases_by_ou = {}
        for row in rows:
            ou_id = row[2]
            period = row[1]
            value = safe_float(row[3])
            
            if ou_id not in cases_by_ou:
                cases_by_ou[ou_id] = {'cases': {}, 'total': 0}
            
            cases_by_ou[ou_id]['cases'][period] = value
            cases_by_ou[ou_id]['total'] += value
        
        print(f"Processed {len(cases_by_ou)} org units")
        
        # Try to fetch populations (but don't fail if not available)
        try:
            current_year = datetime.now().year
            populations = fetch_populations_for_level(auth, level, None, current_year)
            print(f"Got {len(populations)} population records")
        except Exception as pop_err:
            print(f"Population fetch failed: {pop_err}")
            traceback.print_exc()
            populations = {}
        
        # Calculate average incidence for each org unit (for proper ranking)
        # Only include districts WITH population data
        ous_with_population = []
        ous_without_population = []
        
        for ou_id, ou_data in cases_by_ou.items():
            population = populations.get(ou_id)
            if population and population > 0:
                # Calculate average incidence per week
                week_incidences = []
                for period, cases in ou_data['cases'].items():
                    incidence = (cases / population) * 1000
                    week_incidences.append(incidence)
                ou_data['avg_incidence'] = sum(week_incidences) / len(week_incidences) if week_incidences else 0
                ou_data['has_population'] = True
                ous_with_population.append((ou_id, ou_data))
            else:
                # Track districts without population for logging
                ou_info = meta_items.get(ou_id, {})
                ous_without_population.append(ou_info.get('name', ou_id))
        
        # Log districts without population
        if ous_without_population:
            print(f"Districts WITHOUT population data ({len(ous_without_population)}): {', '.join(ous_without_population[:10])}...")
        
        print(f"Districts WITH population: {len(ous_with_population)}")
        
        # Sort org units by average incidence (worst hit at top) - NO LIMIT, show all
        sorted_ous = sorted(ous_with_population, key=lambda x: x[1].get('avg_incidence', 0), reverse=True)
        
        # Build table data
        table_data = []
        for ou_id, ou_data in sorted_ous:
            ou_info = meta_items.get(ou_id, {})
            population = populations.get(ou_id)
            
            row_dict = {
                'orgunit_id': ou_id,
                'orgunit_name': ou_info.get('name', ou_id),
                'population': population,
                'total_cases': int(ou_data['total']),
                'avg_incidence': round(ou_data.get('avg_incidence', 0), 2),
                'weeks': []
            }
            
            for period in periods:
                cases = ou_data['cases'].get(period, 0)
                # Calculate incidence if population available
                if population and population > 0:
                    incidence = (cases / population) * 1000
                else:
                    incidence = None
                
                row_dict['weeks'].append({
                    'period': period,
                    'cases': cases,
                    'incidence': round(incidence, 2) if incidence is not None else None
                })
            
            table_data.append(row_dict)
        
        print(f"Returning {len(table_data)} org units in table")
        
        return jsonify({
            'periods': periods,
            'orgunits': table_data,
            'has_population': len(populations) > 0
        })
    
    except Exception as e:
        print(f"Error in get_incidence_table: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def fetch_orgunit_population(auth, orgunit_id, year):
    """
    Fetch population for a single org unit.
    Uses UBOS_POPULATION (official UBOS data).
    """
    # First, get the org unit name from DHIS2
    try:
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{orgunit_id}",
            auth=auth,
            params={'fields': 'displayName'},
            timeout=30
        )
        
        if response.status_code == 200:
            ou_name = response.json().get('displayName', '')
            print(f"[Population] Looking up: '{ou_name}' (ID: {orgunit_id})")
            
            # Normalize for matching (uppercase, remove common suffixes)
            normalized = ou_name.upper().replace(' DISTRICT', '').replace(' CITY', '').strip()
            
            # Try exact match first
            if normalized in UBOS_POPULATION:
                pop = UBOS_POPULATION[normalized]
                print(f"[Population] Found: {normalized} = {pop:,}")
                return pop
            
            # Try with CITY suffix (for cities like "Kampala" -> "KAMPALA CITY" in UBOS)
            city_name = f"{normalized} CITY"
            if city_name in UBOS_POPULATION:
                pop = UBOS_POPULATION[city_name]
                print(f"[Population] Found: {city_name} = {pop:,}")
                return pop
            
            print(f"[Population] NOT FOUND: '{ou_name}' (tried: {normalized}, {city_name})")
                
    except Exception as e:
        print(f"[Population] Error: {e}")
        traceback.print_exc()
    
    return None
    
    try:
        response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=[
                ('dimension', f'dx:{POPULATION_DE}'),
                ('dimension', f'pe:{year}'),
                ('dimension', f'ou:{orgunit_id}'),
                ('displayProperty', 'NAME')
            ],
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            rows = data.get('rows', [])
            if rows and len(rows) > 0:
                return safe_float(rows[0][3])
    except:
        pass
    
    # Fallback: Try to get from org unit attributes
    try:
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{orgunit_id}",
            auth=auth,
            params={'fields': 'attributeValues[value,attribute[id,name]]'},
            timeout=30
        )
        
        if response.status_code == 200:
            ou_data = response.json()
            attr_values = ou_data.get('attributeValues', [])
            
            for attr in attr_values:
                attr_name = attr.get('attribute', {}).get('name', '').lower()
                if 'population' in attr_name or 'catchment' in attr_name:
                    return safe_float(attr.get('value'))
    except:
        pass
    
    # Default fallback for testing
    return 10000


def find_population_data_element(auth):
    """
    Search for population data element in DHIS2.
    Tries multiple search terms to find UBOS/population data.
    Excludes non-population items like facility counts, rates, etc.
    """
    print("\n=== SEARCHING FOR POPULATION DATA ELEMENT ===")
    
    # Expanded search patterns for Uganda HMIS
    search_patterns = [
        # Name-based searches (most specific first)
        ('displayName', 'UBOS Population'),
        ('displayName', 'Total Population'),
        ('displayName', 'Projected Population'),
        ('displayName', 'Catchment Population'),
        ('displayName', 'District Population'),
        ('displayName', 'population'),
        ('displayName', 'UBOS'),
        ('displayName', 'projected'),
        ('displayName', 'census'),
        # Code-based searches
        ('code', 'W01'),  # Uganda HMIS W01 population data
        ('code', 'POP'),
        ('code', 'UBOS'),
    ]
    
    # Words that indicate this is NOT a population count
    exclude_words = [
        'facility', 'facilities', 'health', 'rate', 'ratio', '%', 'percent', 
        'proportion', 'coverage', 'indicator', 'number of', 'no.', 'no of',
        'staff', 'worker', 'patient', 'visit', 'bed', 'equipment'
    ]
    
    all_found = []
    
    for field, value in search_patterns:
        try:
            filter_param = f'{field}:ilike:{value}'
            response = requests.get(
                f"{DHIS2_BASE_URL}/dataElements",
                auth=auth,
                params={
                    'filter': filter_param,
                    'fields': 'id,code,displayName,valueType',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('dataElements', [])
                
                for el in elements:
                    name_lower = el['displayName'].lower()
                    
                    # Skip if contains excluded words
                    should_exclude = any(excl in name_lower for excl in exclude_words)
                    
                    if not should_exclude and el['id'] not in [e['id'] for e in all_found]:
                        all_found.append(el)
                        print(f"  ✓ Found: {el.get('code', 'N/A')} - {el['displayName']} (ID: {el['id']})")
                    
        except Exception as e:
            print(f"Error searching for population ({field}:{value}): {e}")
            continue
    
    print(f"Total valid population elements found: {len(all_found)}")
    
    if not all_found:
        print("ERROR: No population data element found!")
        print("Please visit: /malaria/api/search-population-elements to see all available options")
        print("==============================================\n")
        return None
    
    # Priority selection - look for best matches first
    priority_patterns = [
        ('ubos', 'population'),       # UBOS Population
        ('total', 'population'),      # Total Population
        ('projected', 'population'),  # Projected Population
        ('catchment', 'population'),  # Catchment Population
        ('district', 'population'),   # District Population
        ('ubos',),                     # UBOS anything
        ('population',),              # Any population
    ]
    
    for patterns in priority_patterns:
        for el in all_found:
            name_lower = el['displayName'].lower()
            if all(p in name_lower for p in patterns):
                print(f"*** SELECTED: {el['displayName']} (ID: {el['id']})")
                print("==============================================\n")
                return el['id']
    
    # Fallback to first numeric element
    for el in all_found:
        if el.get('valueType') in ['NUMBER', 'INTEGER', 'INTEGER_POSITIVE']:
            print(f"*** FALLBACK: {el['displayName']} (ID: {el['id']})")
            print("==============================================\n")
            return el['id']
    
    # Last resort
    print(f"*** LAST RESORT: {all_found[0]['displayName']} (ID: {all_found[0]['id']})")
    print("==============================================\n")
    return all_found[0]['id']


def fetch_populations_for_level(auth, level, parent_id, year):
    """
    Get populations for all org units at a level.
    Uses UBOS_POPULATION from app.py (hardcoded official UBOS data).
    Maps org unit names to population values.
    """
    populations = {}
    
    print(f"\n=== FETCHING POPULATION DATA (UBOS) ===")
    print(f"Level: {level}, UBOS districts available: {len(UBOS_POPULATION)}")
    
    if not UBOS_POPULATION:
        print("ERROR: UBOS_POPULATION not loaded!")
        return populations
    
    # Get org unit names for the level
    try:
        if parent_id:
            ou_dimension = f'ou:{parent_id};LEVEL-{level}'
        else:
            ou_dimension = f'ou:LEVEL-{level}'
        
        # Fetch org units to get their names
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits",
            auth=auth,
            params={
                'level': level,
                'fields': 'id,displayName',
                'paging': 'false'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            org_units = data.get('organisationUnits', [])
            
            print(f"Found {len(org_units)} org units at level {level}")
            
            # Common spelling variations between DHIS2 and UBOS
            SPELLING_MAP = {
                'LUWERO': 'LUWEERO',
                'SEMBABULE': 'SSEMBABULE',
                'SEMBAABULE': 'SSEMBABULE',
                'BUKOMANSIMBI': 'BUKOMANSIMBI',
                'LYANTONDE': 'LYANTONDE',
            }
            
            matched = 0
            unmatched = []
            
            for ou in org_units:
                ou_id = ou['id']
                ou_name = ou['displayName'].upper().strip()
                
                # Try exact match first
                if ou_name in UBOS_POPULATION:
                    populations[ou_id] = UBOS_POPULATION[ou_name]
                    matched += 1
                else:
                    # Try cleaning the name (remove "District", "City", etc.)
                    clean_name = ou_name.replace(' DISTRICT', '').replace(' CITY', '').strip()
                    
                    if clean_name in UBOS_POPULATION:
                        populations[ou_id] = UBOS_POPULATION[clean_name]
                        matched += 1
                    # Try spelling variation
                    elif clean_name in SPELLING_MAP and SPELLING_MAP[clean_name] in UBOS_POPULATION:
                        populations[ou_id] = UBOS_POPULATION[SPELLING_MAP[clean_name]]
                        matched += 1
                    else:
                        # Try with "CITY" suffix
                        city_name = f"{clean_name} CITY"
                        if city_name in UBOS_POPULATION:
                            populations[ou_id] = UBOS_POPULATION[city_name]
                            matched += 1
                        else:
                            unmatched.append(clean_name)
            
            print(f"SUCCESS: Matched {matched}/{len(org_units)} org units to UBOS population")
            
            if unmatched and len(unmatched) <= 10:
                print(f"Unmatched: {', '.join(unmatched[:10])}")
            
            # Show sample
            sample = list(populations.items())[:3]
            for ou_id, pop in sample:
                print(f"  Sample: {ou_id} = {pop:,.0f}")
                
    except Exception as e:
        print(f"Error fetching org units: {e}")
        traceback.print_exc()
    
    print(f"=== END POPULATION FETCH: {len(populations)} records ===\n")
    return populations


def get_orgunit_name(auth, orgunit_id):
    """Get organisation unit name."""
    try:
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{orgunit_id}",
            auth=auth,
            params={'fields': 'displayName'},
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json().get('displayName', orgunit_id)
    except:
        pass
    
    return orgunit_id


@malaria_bp.route('/api/geojson')
@require_login
def get_geojson():
    """
    Fetch GeoJSON boundaries from DHIS2 for map rendering.
    Uses the DHIS2 maps API: https://hmis.health.go.ug/dhis-web-maps/
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        level = request.args.get('level', '3')  # Default to district level
        parent_id = request.args.get('parent')  # Optional parent for filtering
        
        # Build the GeoJSON request URL
        # DHIS2 provides GeoJSON at: /api/organisationUnits.geojson?level=X
        params = {
            'level': level
        }
        
        if parent_id:
            params['parent'] = parent_id
        
        print(f"Fetching GeoJSON for level {level}, parent: {parent_id}")
        
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits.geojson",
            auth=auth,
            params=params,
            timeout=120
        )
        
        print(f"GeoJSON response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"GeoJSON error: {response.text[:500]}")
            return jsonify({'error': f'DHIS2 GeoJSON error: {response.status_code}'}), 500
        
        geojson = response.json()
        
        # Verify it's valid GeoJSON
        if 'features' not in geojson:
            return jsonify({'error': 'Invalid GeoJSON response'}), 500
        
        features = geojson.get('features', [])
        valid_features = [f for f in features if f.get('geometry') and f['geometry'].get('coordinates')]
        
        print(f"Fetched {len(features)} features, {len(valid_features)} have valid geometry")
        
        # Log first feature structure for debugging
        if features:
            sample = features[0]
            print(f"Sample feature: id={sample.get('id')}, has_geometry={sample.get('geometry') is not None}")
            if sample.get('properties'):
                print(f"Sample properties keys: {list(sample['properties'].keys())[:5]}")
        
        return jsonify(geojson)
    
    except Exception as e:
        print(f"Error fetching GeoJSON: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/search-population-elements')
@require_login
def search_population_elements():
    """
    Search for population-related data elements in DHIS2
    Returns a comprehensive list of potential population data sources
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        # Expanded search terms to find population data
        search_terms = [
            'population', 'UBOS', 'W01', 'projected', 'census', 
            'catchment', 'inhabitants', 'people', 'demographic',
            'pop total', 'total pop', 'district pop'
        ]
        
        results = []
        
        for term in search_terms:
            response = requests.get(
                f"{DHIS2_BASE_URL}/dataElements",
                auth=auth,
                params={
                    'filter': f'displayName:ilike:{term}',
                    'fields': 'id,code,displayName,valueType,categoryCombo[name]',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                for element in data.get('dataElements', []):
                    # Avoid duplicates and filter out non-population items
                    name_lower = element['displayName'].lower()
                    # Exclude if it's clearly not population
                    if any(x in name_lower for x in ['rate', 'facility', 'facilities', '%', 'percent', 'proportion']):
                        continue
                    
                    if element['id'] not in [r['id'] for r in results]:
                        results.append({
                            'id': element['id'],
                            'code': element.get('code', ''),
                            'name': element['displayName'],
                            'valueType': element.get('valueType', ''),
                            'category': element.get('categoryCombo', {}).get('name', ''),
                            'matched_term': term
                        })
        
        # Also search indicators
        indicator_results = []
        for term in ['population', 'UBOS', 'projected']:
            response = requests.get(
                f"{DHIS2_BASE_URL}/indicators",
                auth=auth,
                params={
                    'filter': f'displayName:ilike:{term}',
                    'fields': 'id,code,displayName,indicatorType[name]',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                for indicator in data.get('indicators', []):
                    name_lower = indicator['displayName'].lower()
                    if 'rate' not in name_lower and '%' not in name_lower:
                        if indicator['id'] not in [r['id'] for r in indicator_results]:
                            indicator_results.append({
                                'id': indicator['id'],
                                'code': indicator.get('code', ''),
                                'name': indicator['displayName'],
                                'type': 'indicator',
                                'indicatorType': indicator.get('indicatorType', {}).get('name', '')
                            })
        
        # Sort by relevance (UBOS and population keywords first)
        def relevance_score(item):
            name = item['name'].lower()
            score = 0
            if 'ubos' in name: score += 100
            if 'population' in name: score += 50
            if 'projected' in name: score += 40
            if 'total' in name: score += 30
            if 'census' in name: score += 20
            if 'catchment' in name: score += 10
            return -score  # Negative for descending sort
        
        results.sort(key=relevance_score)
        indicator_results.sort(key=relevance_score)
        
        print(f"\n=== POPULATION SEARCH RESULTS ===")
        print(f"Found {len(results)} data elements:")
        for r in results[:10]:  # Show top 10
            print(f"  - {r['code']}: {r['name']} (ID: {r['id']})")
        print(f"Found {len(indicator_results)} indicators:")
        for r in indicator_results[:5]:
            print(f"  - {r['code']}: {r['name']} (ID: {r['id']})")
        print("=================================\n")
        
        return jsonify({
            'data_elements': results,
            'indicators': indicator_results,
            'message': f'Found {len(results)} data elements and {len(indicator_results)} indicators. Check console for recommended element.'
        })
    
    except Exception as e:
        print(f"Error searching population elements: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/search-w01')
@require_login
def search_w01_elements():
    """
    Search specifically for W01 data elements (population data in Uganda HMIS)
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        # Search for W01 code prefix
        response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'code:ilike:W01',
                'fields': 'id,code,displayName,valueType',
                'paging': 'false'
            },
            timeout=30
        )
        
        results = []
        if response.status_code == 200:
            data = response.json()
            results = data.get('dataElements', [])
        
        print(f"\n=== W01 DATA ELEMENTS FOUND ===")
        for el in results:
            print(f"  {el.get('code', 'N/A')}: {el['displayName']} (ID: {el['id']})")
        print(f"Total W01 elements: {len(results)}")
        print("================================\n")
        
        return jsonify({
            'w01_elements': results,
            'count': len(results)
        })
    
    except Exception as e:
        print(f"Error searching W01: {e}")
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/orgunit-children')
@require_login
def get_orgunit_children():
    """
    Get children of an organization unit for drill-down functionality.
    """
    try:
        auth = get_auth()
        if not auth:
            return jsonify({'error': 'Authentication required'}), 401
        
        parent_id = request.args.get('parent')
        if not parent_id:
            return jsonify({'error': 'Parent ID required'}), 400
        
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{parent_id}",
            auth=auth,
            params={
                'fields': 'id,displayName,level,children[id,displayName,level]'
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return jsonify({'error': f'DHIS2 error: {response.status_code}'}), 500
        
        data = response.json()
        children = data.get('children', [])
        
        return jsonify({
            'parent': {
                'id': data.get('id'),
                'name': data.get('displayName'),
                'level': data.get('level')
            },
            'children': children
        })
    
    except Exception as e:
        print(f"Error fetching children: {e}")
        return jsonify({'error': str(e)}), 500
