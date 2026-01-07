"""
WASH Module - Water, Sanitation and Hygiene
Handles CHW household indicators for WASH analytics
"""
from flask import Blueprint, request, jsonify, render_template
import requests
import logging

logger = logging.getLogger(__name__)

from .core import (
    DHIS2_BASE_URL, DHIS2_TIMEOUT, UBOS_POPULATION,
    get_auth, is_logged_in, login_required, http_session,
    analytics_cache, org_units_cache,
    get_period_divisor, generate_monthly_periods, clean_district_name
)

# Create Blueprint
wash_bp = Blueprint('wash', __name__, url_prefix='/wash')

# ============ WASH-SPECIFIC CONFIGURATION ============
# These are pre-calculated indicators in DHIS2 - no calculation needed
WASH_INDICATORS = [
    {
        "name": "Households with Latrines",
        "short_name": "Latrines",
        "search_pattern": "CHW - proportion of households with latrines",
        "target": 75,
        "unit": "%"
    },
    {
        "name": "Households with Improved Latrines",
        "short_name": "Improved Latrines",
        "search_pattern": "CHW - proportion of households with Improved latrines",
        "target": 75,
        "unit": "%"
    },
    {
        "name": "Households with Handwashing Facilities",
        "short_name": "Handwashing",
        "search_pattern": "CHW - proportion of households with handwashing facilities",
        "target": 75,
        "unit": "%"
    },
    {
        "name": "Households with Safe Drinking Water",
        "short_name": "Safe Water",
        "search_pattern": "CHW - proportion of households with source safe drinking water",
        "target": 75,
        "unit": "%"
    },
    {
        "name": "Open Defecation Free Households",
        "short_name": "ODF",
        "search_pattern": "CHW - proportion of households that are open defeacation free",
        "target": 75,
        "unit": "%"
    }
]


def get_wash_color(value):
    """
    Color coding for WASH indicators:
    - Blue: >100% (data error)
    - Green: ≥75%
    - Yellow: 50-74.9%
    - Red: <50%
    """
    if value is None:
        return 'gray'
    if value > 100:
        return 'blue'  # Data error
    if value >= 75:
        return 'green'
    if value >= 50:
        return 'yellow'
    return 'red'


# ============ ROUTES ============
@wash_bp.route('/')
def dashboard():
    """WASH Dashboard page"""
    if not is_logged_in():
        from flask import redirect, url_for
        return redirect(url_for('login'))
    return render_template('wash.html')


@wash_bp.route('/api/indicators')
@login_required
def get_wash_indicators():
    """Search and return WASH indicator IDs from DHIS2"""
    auth = get_auth()
    
    cache_key = analytics_cache._make_key('wash_indicators')
    cached = analytics_cache.get(cache_key)
    if cached:
        return jsonify(cached)
    
    try:
        # Search for indicators matching our patterns
        found_indicators = []
        
        for indicator_config in WASH_INDICATORS:
            pattern = indicator_config['search_pattern']
            
            # Search in indicators
            response = http_session.get(
                f"{DHIS2_BASE_URL}/indicators",
                auth=auth,
                params={
                    'filter': f'displayName:ilike:{pattern}',
                    'fields': 'id,displayName,code',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                indicators = data.get('indicators', [])
                if indicators:
                    # Take the first match
                    ind = indicators[0]
                    found_indicators.append({
                        'id': ind['id'],
                        'displayName': ind['displayName'],
                        'code': ind.get('code', ''),
                        'shortName': indicator_config['short_name'],
                        'name': indicator_config['name'],
                        'target': indicator_config['target'],
                        'unit': indicator_config['unit']
                    })
                    logger.info(f"Found WASH indicator: {ind['displayName']} -> {ind['id']}")
                else:
                    logger.warning(f"No indicator found for pattern: {pattern}")
        
        result = {
            'indicators': found_indicators,
            'count': len(found_indicators)
        }
        
        analytics_cache.set(cache_key, result)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error fetching WASH indicators: {e}")
        return jsonify({'error': str(e)})


@wash_bp.route('/api/analytics-data')
@login_required
def get_analytics_data():
    """Get WASH analytics data for an org unit"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    period = request.args.get('period', 'LAST_12_MONTHS')
    
    # Check cache
    cache_key = analytics_cache._make_key('wash_analytics', org_unit, period)
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    try:
        # First get the indicator IDs
        indicators_response = http_session.get(
            f"{DHIS2_BASE_URL}/indicators",
            auth=auth,
            params={
                'filter': 'displayName:ilike:CHW - proportion of households',
                'fields': 'id,displayName,code',
                'paging': 'false'
            },
            timeout=30
        )
        
        if indicators_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch indicators'})
        
        all_indicators = indicators_response.json().get('indicators', [])
        
        # Map indicators to our config
        indicator_map = {}
        for config in WASH_INDICATORS:
            pattern = config['search_pattern'].lower()
            for ind in all_indicators:
                if pattern in ind['displayName'].lower():
                    indicator_map[ind['id']] = {
                        **config,
                        'dhis2_id': ind['id'],
                        'dhis2_name': ind['displayName']
                    }
                    break
        
        if not indicator_map:
            return jsonify({
                'error': 'No WASH indicators found in DHIS2',
                'tip': 'Check if CHW indicators are available in your DHIS2 instance'
            })
        
        # Build analytics query
        indicator_ids = list(indicator_map.keys())
        dx_dimension = ";".join(indicator_ids)
        
        # Handle period
        if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
            start, end = period.split('-')
            periods = generate_monthly_periods(start, end)
        elif ';' in period:
            periods = period
        else:
            periods = period
        
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{periods}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=DHIS2_TIMEOUT
        )
        
        if data_response.status_code != 200:
            logger.error(f"DHIS2 Analytics error: {data_response.status_code}")
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        
        data = data_response.json()
        
        # Parse results
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        # Aggregate values by indicator (average across periods)
        indicator_values = {}
        indicator_counts = {}
        
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            try:
                value = float(row[val_idx]) if len(row) > val_idx else 0
            except (ValueError, TypeError):
                value = 0
            
            if dx_id:
                if dx_id not in indicator_values:
                    indicator_values[dx_id] = 0
                    indicator_counts[dx_id] = 0
                indicator_values[dx_id] += value
                indicator_counts[dx_id] += 1
        
        # Calculate averages and build result
        results = []
        for ind_id, config in indicator_map.items():
            count = indicator_counts.get(ind_id, 0)
            total = indicator_values.get(ind_id, 0)
            avg_value = round(total / count, 1) if count > 0 else None
            
            results.append({
                'id': ind_id,
                'name': config['name'],
                'shortName': config['short_name'],
                'value': avg_value,
                'target': config['target'],
                'unit': config['unit'],
                'color': get_wash_color(avg_value),
                'isDataError': avg_value is not None and avg_value > 100,
                'periodsWithData': count
            })
        
        # Get org unit name
        org_response = http_session.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{org_unit}",
            auth=auth,
            params={'fields': 'id,displayName,level'},
            timeout=30
        )
        org_name = ''
        org_level = 1
        if org_response.status_code == 200:
            org_data = org_response.json()
            org_name = org_data.get('displayName', '')
            org_level = org_data.get('level', 1)
        
        result = {
            'orgUnit': org_name,
            'orgLevel': org_level,
            'period': period,
            'indicators': results,
            'indicatorsFound': len(indicator_map),
            'rowsReturned': len(data.get('rows', [])),
            '_cached': False
        }
        
        analytics_cache.set(cache_key, result)
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try a smaller time period'})
    except Exception as e:
        logger.error(f"Error in get_analytics_data: {e}")
        return jsonify({'error': str(e)})


@wash_bp.route('/api/compare-data')
@login_required
def get_compare_data():
    """Get WASH data for comparison across multiple org units"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit')
    period = request.args.get('period', 'LAST_12_MONTHS')
    district_name = request.args.get('districtName', '')
    
    if not org_unit:
        return jsonify({'error': 'Organization unit is required'})
    
    try:
        # Get indicator IDs
        indicators_response = http_session.get(
            f"{DHIS2_BASE_URL}/indicators",
            auth=auth,
            params={
                'filter': 'displayName:ilike:CHW - proportion of households',
                'fields': 'id,displayName,code',
                'paging': 'false'
            },
            timeout=30
        )
        
        if indicators_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch indicators'})
        
        all_indicators = indicators_response.json().get('indicators', [])
        
        # Map indicators
        indicator_map = {}
        for config in WASH_INDICATORS:
            pattern = config['search_pattern'].lower()
            for ind in all_indicators:
                if pattern in ind['displayName'].lower():
                    indicator_map[ind['id']] = config
                    break
        
        if not indicator_map:
            return jsonify({'indicators': [], 'hasData': False})
        
        # Build analytics query
        indicator_ids = list(indicator_map.keys())
        dx_dimension = ";".join(indicator_ids)
        
        # Handle period
        if ';' in period:
            periods = period
        elif '-' in period and not period.startswith('LAST'):
            start, end = period.split('-')
            periods = generate_monthly_periods(start, end)
        else:
            periods = period
        
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{periods}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=DHIS2_TIMEOUT
        )
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        
        data = data_response.json()
        
        # Parse results
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        # Aggregate by indicator
        indicator_values = {}
        indicator_counts = {}
        
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            try:
                value = float(row[val_idx]) if len(row) > val_idx else 0
            except:
                value = 0
            
            if dx_id:
                if dx_id not in indicator_values:
                    indicator_values[dx_id] = 0
                    indicator_counts[dx_id] = 0
                indicator_values[dx_id] += value
                indicator_counts[dx_id] += 1
        
        # Build results keyed by short name
        results = {}
        for ind_id, config in indicator_map.items():
            count = indicator_counts.get(ind_id, 0)
            total = indicator_values.get(ind_id, 0)
            avg_value = round(total / count, 1) if count > 0 else None
            
            key = config['short_name'].lower().replace(' ', '_')
            results[key] = avg_value
            results[f"{key}_color"] = get_wash_color(avg_value)
        
        results['hasData'] = len(data.get('rows', [])) > 0
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Error in get_compare_data: {e}")
        return jsonify({'error': str(e)})

