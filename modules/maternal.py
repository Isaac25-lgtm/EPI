"""
Maternal Health Module - ANC Indicators
Handles Antenatal Care data from DHIS2 HMIS 105 forms

Formulas:
- ANC Catchment = 5% of UBOS population (districts) or 5% of catchment population (facilities)
- ANC 1 Coverage = (105-AN01a / ANC Catchment) * 100
- ANC 1st Trimester = (105-AN01b / 105-AN01a) * 100
- ANC 4 Coverage = (105-AN02 / ANC Catchment) * 100
- ANC 8 Coverage = (105-AN03 / ANC Catchment) * 100
- IPT3 Coverage = (105-AN06c / ANC Catchment) * 100
- Hb Testing = (105-AN08 / 105-AN01a) * 100
- LLIN at ANC 1 = (105-AN11 / 105-AN01a) * 100
- Teenage Preg = ((ANC <15yrs + ANC 15-19yrs) / 105-AN01a) * 100
- Iron/Folic = (105-AN21 / 105-AN01a) * 100
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
        DHIS2_BASE_URL, UBOS_POPULATION
    )
except ImportError:
    DHIS2_BASE_URL = "https://hmis.health.go.ug/api"
    UBOS_POPULATION = {}
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
maternal_bp = Blueprint('maternal', __name__, url_prefix='/maternal')

# ANC Data Element search patterns and expected codes
ANC_INDICATORS = [
    {'code': '105-AN01a', 'name': 'ANC 1st Visit', 'key': 'anc1Visit'},
    {'code': '105-AN01b', 'name': 'ANC 1st Visit 1st Trimester', 'key': 'anc1stTrimester'},
    {'code': '105-AN02', 'name': 'ANC 4th Visit', 'key': 'anc4'},
    {'code': '105-AN03', 'name': 'ANC 8+ Visits', 'key': 'anc8'},
    {'code': '105-AN06c', 'name': 'IPT3', 'key': 'ipt3'},
    {'code': '105-AN08', 'name': 'Hb Test', 'key': 'hbTest'},
    {'code': '105-AN11', 'name': 'LLIN at ANC 1', 'key': 'llin'},
    {'code': '105-AN21', 'name': 'Iron/Folic Acid', 'key': 'ironFolic'},
]

# Cache for data element IDs
_data_element_cache = {}


def clean_district_name(name):
    """Clean district name to match UBOS format"""
    if not name:
        return ''
    clean = name.upper()
    for suffix in [' DISTRICT', ' CITY', ' MUNICIPAL COUNCIL', ' MUNICIPALITY']:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)]
    return clean.strip()


def get_ubos_population(district_name):
    """Get UBOS population for a district with fuzzy matching"""
    if not district_name:
        return 0
    
    clean = clean_district_name(district_name)
    
    # Try exact match
    if clean in UBOS_POPULATION:
        return UBOS_POPULATION[clean]
    
    # Try with CITY suffix
    if f"{clean} CITY" in UBOS_POPULATION:
        return UBOS_POPULATION[f"{clean} CITY"]
    
    # Try without CITY if it has it
    base = clean.replace(' CITY', '').strip()
    if base in UBOS_POPULATION:
        return UBOS_POPULATION[base]
    
    return 0


def fetch_data_element_ids(auth):
    """Fetch all ANC data element IDs from DHIS2"""
    global _data_element_cache
    
    if _data_element_cache:
        return _data_element_cache
    
    try:
        # Search for all ANC data elements (105-AN prefix)
        response = http_session.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'name:ilike:105-AN',
                'fields': 'id,displayName,code,name',
                'paging': 'false'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            elements = response.json().get('dataElements', [])
            for elem in elements:
                # Store by code, name, and displayName for flexible matching
                code = elem.get('code', '')
                name = elem.get('name', '')
                display_name = elem.get('displayName', '')
                
                if code:
                    _data_element_cache[code] = elem['id']
                if name:
                    _data_element_cache[name] = elem['id']
                if display_name:
                    _data_element_cache[display_name] = elem['id']
            
            logger.info(f"Cached {len(elements)} ANC data elements")
            
            # Log age-specific elements if found
            teen_elements = [k for k in _data_element_cache.keys() 
                           if '<15' in k or '15-19' in k or 'teen' in k.lower()]
            if teen_elements:
                logger.info(f"Found teenage pregnancy elements: {teen_elements[:5]}")
            else:
                logger.warning("No teenage pregnancy age-disaggregated elements found")
        else:
            logger.error(f"Failed to fetch data elements: {response.status_code}")
        
        return _data_element_cache
    except Exception as e:
        logger.error(f"Error fetching data elements: {e}")
        return {}


# Routes
@maternal_bp.route('/')
def dashboard():
    """Maternal Health Dashboard page"""
    if not is_logged_in():
        return redirect(url_for('auth.login'))
    return render_template('maternal.html')


@maternal_bp.route('/api/anc-data')
def get_anc_data():
    """
    Fetch ANC data from DHIS2 and calculate all indicators
    """
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit')
    period = request.args.get('period', 'LAST_12_MONTHS')
    custom_population = request.args.get('customPopulation', type=int)
    custom_anc_catchment = request.args.get('ancCatchment', type=int)
    
    if not org_unit:
        return jsonify({'error': 'Organization unit is required'})
    
    try:
        # Get organization unit details
        org_response = http_session.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{org_unit}",
            auth=auth,
            params={'fields': 'id,displayName,level,ancestors[displayName,level]'},
            timeout=30
        )
        
        if org_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch organization unit details'})
        
        org_data = org_response.json()
        org_name = org_data.get('displayName', '')
        org_level = org_data.get('level', 5)
        
        # Find district name from hierarchy
        district_name = ''
        if org_level == 3:
            district_name = org_name
        elif org_data.get('ancestors'):
            for ancestor in org_data['ancestors']:
                if ancestor.get('level') == 3:
                    district_name = ancestor.get('displayName', '')
                    break
        
        # Determine population and ANC catchment
        if custom_anc_catchment and custom_anc_catchment > 0:
            # Use pre-calculated period-adjusted catchment from frontend
            population = custom_population or 0
            anc_catchment = custom_anc_catchment
            logger.info(f"Using pre-calculated ANC catchment: {anc_catchment}")
        elif custom_population and custom_population > 0:
            population = custom_population
            anc_catchment = round(population * 0.05)
        elif org_level <= 3:  # District level
            population = get_ubos_population(district_name or org_name)
            anc_catchment = round(population * 0.05) if population > 0 else 0
        else:
            # Facility - needs custom population
            population = 0
            anc_catchment = 0
        
        # Get data element IDs
        de_ids = fetch_data_element_ids(auth)
        
        logger.info(f"Found {len(de_ids)} data element entries")
        
        if not de_ids:
            return jsonify({
                'error': 'No ANC data elements found in DHIS2',
                'tip': 'Check if you have access to HMIS 105 ANC data elements'
            })
        
        # Map of DHIS2 element names/codes to our internal keys
        element_patterns = {
            'anc1Visit': ['105-AN01a', '105-AN01a.', 'ANC 1st Visit for women', 
                          '105-AN01a. ANC 1st Visit for women'],
            'anc1stTrimester': ['105-AN01b', '105-AN01b.', 'ANC 1st Trimester',
                                '105-AN01b. ANC 1st contacts/ visits for women - No. in 1st Trimester'],
            'anc4': ['105-AN02', '105-AN02.', 'ANC 4th Visit', 
                     '105-AN02. ANC 4th Visit for women'],
            'anc8': ['105-AN03', '105-AN03.', 'ANC 8', 
                     '105-AN03. ANC 8 contacts/ visits for Women'],
            'ipt3': ['105-AN06c', '105-AN06c.', 'IPT3',
                     '105-AN06c. No. of pregnant women who received IPT3'],
            'hbTest': ['105-AN08', '105-AN08.', 'Hb Test', 'Anaemia',
                       '105-AN08. No. of pregnant women who were tested for Anaemia'],
            'llin': ['105-AN11', '105-AN11.', 'LLIN',
                     '105-AN11. Pregnant Women receiving LLINs at ANC 1st visit'],
            'ironFolic': ['105-AN21', '105-AN21_2019', '105-AN21a', 'Iron/Folic',
                          '105-AN21_2019 Pregnant Women receiving atleast 30 tablets of Iron/Folic Acid'],
            # Teenage pregnancy - under 15 years
            'teenUnder15': ['105-AN01a. ANC 1st Visit for women <15Yrs',
                            '105-AN01a <15', 'ANC 1st Visit <15',
                            '105-AN01a. ANC 1st Visit for women - <15Yrs'],
            # Teenage pregnancy - 15-19 years
            'teen15_19': ['105-AN01a. ANC 1st Visit for women 15-19Yrs',
                          '105-AN01a 15-19', 'ANC 1st Visit 15-19',
                          '105-AN01a. ANC 1st Visit for women - 15-19Yrs'],
        }
        
        ids_to_fetch = []
        code_to_key = {}
        found_elements = {}
        
        for key, patterns in element_patterns.items():
            for pattern in patterns:
                if pattern in de_ids:
                    elem_id = de_ids[pattern]
                    if elem_id not in ids_to_fetch:
                        ids_to_fetch.append(elem_id)
                        code_to_key[elem_id] = key
                        found_elements[key] = pattern
                    break
        
        logger.info(f"Found elements: {found_elements}")
        
        if not ids_to_fetch:
            # Return available codes for debugging
            sample_codes = list(de_ids.keys())[:30]
            return jsonify({
                'error': 'Could not find ANC data element IDs',
                'availableCodes': sample_codes,
                'tip': 'Check if data element names match expected patterns'
            })
        
        # Fetch analytics data
        dx_dimension = ";".join(ids_to_fetch)
        
        # Use list of tuples for multiple dimension parameters
        analytics_response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics.json",
            auth=auth,
            params=[
                ('dimension', f'dx:{dx_dimension}'),
                ('dimension', f'pe:{period}'),
                ('dimension', f'ou:{org_unit}'),
                ('displayProperty', 'NAME'),
                ('skipMeta', 'false'),
            ],
            timeout=60
        )
        
        # Handle the response
        if analytics_response.status_code != 200:
            logger.warning(f"Analytics first attempt failed: {analytics_response.status_code}")
            logger.warning(f"Response: {analytics_response.text[:500]}")
            return jsonify({
                'error': f'Analytics API error: {analytics_response.status_code}',
                'details': analytics_response.text[:200]
            })
        
        analytics_data = analytics_response.json()
        rows = analytics_data.get('rows', [])
        headers = analytics_data.get('headers', [])
        
        # Find column indices
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), len(headers) - 1)
        
        # Aggregate values by data element
        totals = {}
        for row in rows:
            if len(row) > max(dx_idx, val_idx):
                dx_id = row[dx_idx]
                try:
                    value = float(row[val_idx])
                except (ValueError, TypeError):
                    value = 0
                totals[dx_id] = totals.get(dx_id, 0) + value
        
        # Map back to indicator keys
        raw_values = {
            'anc1Visit': 0,
            'anc1stTrimester': 0,
            'anc4': 0,
            'anc8': 0,
            'ipt3': 0,
            'hbTest': 0,
            'llin': 0,
            'ironFolic': 0,
            'teenUnder15': 0,
            'teen15_19': 0,
        }
        
        for de_id, value in totals.items():
            key = code_to_key.get(de_id, '')
            if key and key in raw_values:
                raw_values[key] = int(value)
        
        logger.info(f"Raw values: {raw_values}")
        
        # Define anc1 early for use in calculations
        anc1 = raw_values['anc1Visit']
        
        # If teenage pregnancy data not found, try to fetch disaggregated data
        if raw_values['teenUnder15'] == 0 and raw_values['teen15_19'] == 0 and anc1 > 0:
            logger.info("Attempting to fetch teenage pregnancy disaggregated data...")
            try:
                # Try fetching with age disaggregation
                # First, find the ANC 1st Visit data element ID
                anc1_id = None
                for key, elem_id in code_to_key.items():
                    if elem_id == 'anc1Visit':
                        anc1_id = key
                        break
                
                if anc1_id:
                    # Fetch with categoryOptionCombo dimension
                    teen_response = http_session.get(
                        f"{DHIS2_BASE_URL}/analytics.json",
                        auth=auth,
                        params=[
                            ('dimension', f'dx:{anc1_id}'),
                            ('dimension', f'pe:{period}'),
                            ('dimension', f'ou:{org_unit}'),
                            ('dimension', 'co'),  # Category option combos
                        ],
                        timeout=60
                    )
                    
                    if teen_response.status_code == 200:
                        teen_data = teen_response.json()
                        teen_rows = teen_data.get('rows', [])
                        meta_items = teen_data.get('metaData', {}).get('items', {})
                        
                        for row in teen_rows:
                            if len(row) >= 4:
                                co_id = row[1]  # Category option combo ID
                                co_name = meta_items.get(co_id, {}).get('name', '')
                                try:
                                    value = float(row[-1])
                                except:
                                    value = 0
                                
                                # Check for teenage age groups
                                co_lower = co_name.lower()
                                if '<15' in co_name or 'under 15' in co_lower or '10-14' in co_name:
                                    raw_values['teenUnder15'] += int(value)
                                    logger.info(f"Found <15 years: {co_name} = {value}")
                                elif '15-19' in co_name:
                                    raw_values['teen15_19'] += int(value)
                                    logger.info(f"Found 15-19 years: {co_name} = {value}")
                        
                        # Recalculate teenage pregnancy rate
                        if raw_values['teenUnder15'] > 0 or raw_values['teen15_19'] > 0:
                            teen_preg_rate = round(((raw_values['teenUnder15'] + raw_values['teen15_19']) / anc1 * 100), 1)
                            logger.info(f"Updated teenage pregnancy rate: {teen_preg_rate}%")
            except Exception as e:
                logger.warning(f"Could not fetch teenage disaggregated data: {e}")
        
        # Calculate indicators
        # (anc1 already defined above)
        
        # Population-based indicators (use ANC catchment)
        anc1_coverage = round((anc1 / anc_catchment * 100), 1) if anc_catchment > 0 else 0
        anc4_coverage = round((raw_values['anc4'] / anc_catchment * 100), 1) if anc_catchment > 0 else 0
        anc8_coverage = round((raw_values['anc8'] / anc_catchment * 100), 1) if anc_catchment > 0 else 0
        ipt3_coverage = round((raw_values['ipt3'] / anc_catchment * 100), 1) if anc_catchment > 0 else 0
        
        # Non-population based (use ANC 1 as denominator)
        anc1st_tri_rate = round((raw_values['anc1stTrimester'] / anc1 * 100), 1) if anc1 > 0 else 0
        hb_test_rate = round((raw_values['hbTest'] / anc1 * 100), 1) if anc1 > 0 else 0
        llin_rate = round((raw_values['llin'] / anc1 * 100), 1) if anc1 > 0 else 0
        iron_folic_rate = round((raw_values['ironFolic'] / anc1 * 100), 1) if anc1 > 0 else 0
        teen_preg_rate = round(((raw_values['teenUnder15'] + raw_values['teen15_19']) / anc1 * 100), 1) if anc1 > 0 else 0
        
        result = {
            # Context
            'population': population,
            'ancCatchment': anc_catchment,
            'orgUnit': org_name,
            'orgLevel': org_level,
            'districtName': district_name,
            'period': period,
            
            # Raw values
            **raw_values,
            
            # Calculated coverages (%)
            'anc1Coverage': anc1_coverage,
            'anc1stTriRate': anc1st_tri_rate,
            'anc4Coverage': anc4_coverage,
            'anc8Coverage': anc8_coverage,
            'ipt3Coverage': ipt3_coverage,
            'hbTestRate': hb_test_rate,
            'llinRate': llin_rate,
            'ironFolicRate': iron_folic_rate,
            'teenPregRate': teen_preg_rate,
            
            # Debug info
            'dataElementsFound': len(ids_to_fetch),
            'rowsReturned': len(rows),
            'elementsMatched': list(found_elements.keys()),
            'debug': {
                'foundElements': found_elements,
                'totalsByDE': {code_to_key.get(k, k): v for k, v in totals.items()}
            }
        }
        
        logger.info(f"Returning ANC data: anc1={anc1}, catchment={anc_catchment}, coverage={anc1_coverage}%")
        
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try a smaller time period'})
    except Exception as e:
        logger.error(f"Error in get_anc_data: {e}")
        return jsonify({'error': str(e)})


@maternal_bp.route('/api/league-table')
def get_league_table():
    """Generate league table comparison for multiple org units"""
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_units = request.args.getlist('orgUnits[]')
    period = request.args.get('period', 'LAST_12_MONTHS')
    populations_json = request.args.get('populations', '{}')
    
    if not org_units or len(org_units) < 2:
        return jsonify({'error': 'At least 2 organization units required'})
    
    try:
        import json
        population_map = json.loads(populations_json)
        
        league_data = []
        
        for org_unit_id in org_units:
            # Get org unit details
            org_response = http_session.get(
                f"{DHIS2_BASE_URL}/organisationUnits/{org_unit_id}",
                auth=auth,
                params={'fields': 'id,displayName,level'},
                timeout=30
            )
            
            if org_response.status_code != 200:
                continue
            
            org_data = org_response.json()
            org_name = org_data.get('displayName', org_unit_id)
            
            # Use provided population or fetch from UBOS
            custom_pop = population_map.get(org_unit_id)
            
            league_data.append({
                'orgUnitId': org_unit_id,
                'orgUnit': org_name,
                'population': custom_pop or 0,
                'needsPopulation': custom_pop is None and org_data.get('level', 5) > 3,
                'anc1': 0,
                'anc4': 0,
                'score': 0
            })
        
        # Sort by score
        league_data.sort(key=lambda x: x['score'], reverse=True)
        
        # Add ranks
        for idx, item in enumerate(league_data):
            item['rank'] = idx + 1
        
        return jsonify({'leagueTable': league_data})
        
    except Exception as e:
        logger.error(f"Error in league table: {e}")
        return jsonify({'error': str(e)})


@maternal_bp.route('/api/data-elements')
def list_data_elements():
    """List available ANC data elements from DHIS2 - for debugging"""
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    search = request.args.get('search', '105-AN')
    
    try:
        response = http_session.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': f'name:ilike:{search}',
                'fields': 'id,displayName,code,name,categoryCombo[name,categoryOptionCombos[id,name]]',
                'paging': 'false'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            elements = response.json().get('dataElements', [])
            # Simplify the response
            result = []
            for elem in elements:
                cat_combo = elem.get('categoryCombo', {})
                options = cat_combo.get('categoryOptionCombos', [])
                result.append({
                    'id': elem['id'],
                    'name': elem.get('name', ''),
                    'displayName': elem.get('displayName', ''),
                    'code': elem.get('code', ''),
                    'categoryCombo': cat_combo.get('name', ''),
                    'disaggregations': [opt.get('name', '') for opt in options[:10]]  # Limit to 10
                })
            
            # Look for teenage pregnancy elements specifically
            teen_elements = [e for e in result if '<15' in str(e) or '15-19' in str(e) or 
                           any('<15' in d or '15-19' in d for d in e.get('disaggregations', []))]
            
            return jsonify({
                'total': len(result),
                'elements': result[:50],  # Limit to 50
                'teenageElements': teen_elements
            })
        
        return jsonify({'error': f'Failed to fetch: {response.status_code}'})
    except Exception as e:
        logger.error(f"Error listing data elements: {e}")
        return jsonify({'error': str(e)})
