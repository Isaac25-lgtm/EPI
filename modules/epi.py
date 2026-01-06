"""
EPI Module - Expanded Programme on Immunization
Handles all immunization analytics, coverage, dropouts, RED categorization
"""
from flask import Blueprint, request, jsonify, render_template
from calendar import month_abbr
import requests

from .core import (
    DHIS2_BASE_URL, DHIS2_TIMEOUT, UBOS_POPULATION,
    get_auth, is_logged_in, login_required, http_session,
    analytics_cache, search_cache, org_units_cache, data_elements_cache,
    fetch_org_units, fetch_data_elements,
    get_period_divisor, calculate_coverage, get_coverage_color,
    calculate_dropout, generate_monthly_periods, detect_outliers_zscore,
    simple_forecast, clean_district_name
)

# Create Blueprint
epi_bp = Blueprint('epi', __name__, url_prefix='/epi')

# ============ EPI-SPECIFIC CONFIGURATION ============
TARGET_PERCENTAGES = {
    "BCG": 4.85, "OPV0": 4.85, "HEPB_BIRTH": 4.85,
    "OPV1": 4.3, "OPV2": 4.3, "OPV3": 4.3,
    "DPT1": 4.3, "DPT2": 4.3, "DPT3": 4.3,
    "PCV1": 4.3, "PCV2": 4.3, "PCV3": 4.3,
    "ROTA1": 4.3, "ROTA2": 4.3, "ROTA3": 4.3,
    "IPV1": 4.3, "IPV2": 4.3,
    "MALARIA1": 4.3, "MALARIA2": 4.3, "MALARIA3": 4.3, "MALARIA4": 4.3,
    "MR1": 4.3, "MR2": 4.3, "YELLOW_FEVER": 4.3,
    "FULLY_IMMUNIZED_1YR": 4.3, "FULLY_IMMUNIZED_2YR": 4.3,
    "LLINS": 4.3, "PAB": 4.85, "DEFAULT": 4.3
}

CODE_TO_TARGET = {
    "105-CL01": "BCG", "105-CL02": "HEPB_BIRTH", "105-CL03": "PAB",
    "105-CL04": "OPV0", "105-CL05": "OPV1", "105-CL06": "OPV2", "105-CL07": "OPV3",
    "105-CL08": "IPV1", "105-CL09": "IPV2",
    "105-CL10": "DPT1", "105-CL11": "DPT2", "105-CL12": "DPT3",
    "105-CL13": "PCV1", "105-CL14": "PCV2", "105-CL15": "PCV3",
    "105-CL16": "ROTA1", "105-CL17": "ROTA2", "105-CL18": "ROTA3",
    "105-CL19": "MALARIA1", "105-CL20": "MALARIA2", "105-CL21": "MALARIA3",
    "105-CL22": "YELLOW_FEVER", "105-CL23": "MR1",
    "105-CL24": "FULLY_IMMUNIZED_1YR", "105-CL25": "LLINS",
    "105-CL26": "MALARIA4", "105-CL27": "MR2", "105-CL28": "FULLY_IMMUNIZED_2YR"
}

DROPOUT_CONFIGS = [
    {"name": "DPT1→DPT3", "first": "105-CL10", "last": "105-CL12"},
    {"name": "Polio1→Polio3", "first": "105-CL05", "last": "105-CL07"},
    {"name": "BCG→MR1", "first": "105-CL01", "last": "105-CL23"},
    {"name": "PCV1→PCV3", "first": "105-CL13", "last": "105-CL15"},
    {"name": "Rota1→Rota3", "first": "105-CL16", "last": "105-CL18"},
    {"name": "Malaria1→Malaria2", "first": "105-CL19", "last": "105-CL20"},
    {"name": "Malaria2→Malaria3", "first": "105-CL20", "last": "105-CL21"},
    {"name": "Malaria3→Malaria4", "first": "105-CL21", "last": "105-CL26"},
    {"name": "Malaria1→Malaria4", "first": "105-CL19", "last": "105-CL26"},
]


# ============ ROUTES ============
@epi_bp.route('/')
def dashboard():
    """EPI Dashboard page"""
    if not is_logged_in():
        from flask import redirect, url_for
        return redirect(url_for('auth.login'))
    return render_template('dashboard.html')


@epi_bp.route('/api/analytics-data')
@login_required
def get_analytics_data():
    """Get EPI analytics data with coverage calculations"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    period = request.args.get('period', 'LAST_12_MONTHS')
    custom_population = request.args.get('customPopulation', None)
    
    # Check cache
    cache_key = analytics_cache._make_key('epi_analytics', org_unit, district_name, period, custom_population or '')
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    divisor = get_period_divisor(period)
    
    # Get population
    if custom_population and custom_population.isdigit():
        population = int(custom_population)
    else:
        population = UBOS_POPULATION.get(district_name, 0)
    
    if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
        start, end = period.split('-')
        periods = generate_monthly_periods(start, end)
        period_count = len(periods.split(';'))
        divisor = 12 / period_count if period_count < 12 else 1
    else:
        periods = period
    
    try:
        elements_data = fetch_data_elements(auth, '105-CL')
        if 'error' in elements_data:
            return jsonify(elements_data)
        
        elements = elements_data.get('dataElements', [])
        ids = [e['id'] for e in elements]
        code_map = {e['id']: e['code'] for e in elements}
        
        dx_dimension = ";".join(ids)
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
        
        if data_response.status_code == 200:
            data = data_response.json()
            
            analytics_result = {
                'population': population,
                'divisor': divisor,
                'indicators': [],
                'dropouts': [],
                'raw': data
            }
            
            headers = data.get('headers', [])
            dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
            val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
            if val_idx == -1: val_idx = len(headers) - 1
            
            indicator_totals = {}
            for row in data.get('rows', []):
                dx_id = row[dx_idx] if len(row) > dx_idx else ''
                try:
                    value = int(float(row[val_idx])) if len(row) > val_idx else 0
                except: value = 0
                if dx_id: indicator_totals[dx_id] = indicator_totals.get(dx_id, 0) + value
            
            for dx_id, total in indicator_totals.items():
                code = code_map.get(dx_id, '')
                target_key = CODE_TO_TARGET.get(code, 'DEFAULT')
                target_pct = TARGET_PERCENTAGES.get(target_key, 4.3)
                coverage = calculate_coverage(total, population, target_pct, divisor)
                
                analytics_result['indicators'].append({
                    'id': dx_id, 'code': code,
                    'name': data.get('metaData', {}).get('items', {}).get(dx_id, {}).get('name', code),
                    'doses': total,
                    'target_population': round((population * target_pct / 100) / divisor),
                    'coverage': coverage,
                    'color': get_coverage_color(coverage)
                })
            
            for config in DROPOUT_CONFIGS:
                first_id = next((e['id'] for e in elements if e['code'] == config['first']), None)
                last_id = next((e['id'] for e in elements if e['code'] == config['last']), None)
                if first_id and last_id:
                    first_doses = indicator_totals.get(first_id, 0)
                    last_doses = indicator_totals.get(last_id, 0)
                    dropout = calculate_dropout(first_doses, last_doses)
                    analytics_result['dropouts'].append({
                        'name': config['name'],
                        'first_doses': first_doses,
                        'last_doses': last_doses,
                        'dropout_rate': dropout,
                        'color': 'red' if dropout >= 10 else 'green'
                    })
            
            analytics_result['_cached'] = False
            analytics_cache.set(cache_key, analytics_result)
            return jsonify(analytics_result)
        
        return jsonify({'error': f'Analytics error: {data_response.status_code}'})
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try again or select a smaller time period'})
    except Exception as e:
        return jsonify({'error': str(e)})


@epi_bp.route('/api/red-categorization')
@login_required
def red_categorization():
    """RED Categorization Tool - Quarterly analysis"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    custom_population = request.args.get('customPopulation', None)
    start_date = request.args.get('startDate', None)
    end_date = request.args.get('endDate', None)
    
    # Check cache
    cache_key = analytics_cache._make_key('red_cat', org_unit, custom_population or '', start_date or '', end_date or '')
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    def generate_quarters(start, end):
        """Generate quarters between start and end dates"""
        quarters = []
        start_y, start_m = map(int, start.split('-'))
        end_y, end_m = map(int, end.split('-'))
        
        current_y, current_m = start_y, start_m
        months_list = []
        
        while (current_y < end_y) or (current_y == end_y and current_m <= end_m):
            months_list.append((current_y, current_m))
            current_m += 1
            if current_m > 12:
                current_m = 1
                current_y += 1
        
        i = 0
        quarter_num = 1
        while i < len(months_list):
            quarter_months = months_list[i:i+3]
            if not quarter_months:
                break
            
            month_codes = [f"{y}{m:02d}" for y, m in quarter_months]
            month_names = [month_abbr[m] for y, m in quarter_months]
            
            if len(quarter_months) == 3:
                q_name = f"Q{quarter_num} {quarter_months[0][0]}"
                q_display = f"{month_names[0]}-{month_names[2]}"
            else:
                q_name = f"{month_names[0]}-{month_names[-1]} {quarter_months[0][0]}"
                q_display = "-".join(month_names)
            
            quarters.append({
                'name': q_name,
                'display': q_display,
                'months': ';'.join(month_codes),
                'period': f"{quarter_months[0][0]}Q{quarter_num}"
            })
            
            i += 3
            quarter_num += 1
            if quarter_num > 4:
                quarter_num = 1
        
        return quarters
    
    if start_date and end_date:
        try:
            quarters = generate_quarters(start_date, end_date)
            if not quarters:
                return jsonify({'error': 'Invalid date range'})
        except:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM'})
    else:
        quarters = [
            {'name': 'Q3 2025', 'display': 'Jul-Sep', 'period': '2025Q3', 'months': '202507;202508;202509'},
            {'name': 'Q2 2025', 'display': 'Apr-Jun', 'period': '2025Q2', 'months': '202504;202505;202506'},
            {'name': 'Q1 2025', 'display': 'Jan-Mar', 'period': '2025Q1', 'months': '202501;202502;202503'},
            {'name': 'Q4 2024', 'display': 'Oct-Dec', 'period': '2024Q4', 'months': '202410;202411;202412'},
            {'name': 'Q3 2024', 'display': 'Jul-Sep', 'period': '2024Q3', 'months': '202407;202408;202409'},
            {'name': 'Q2 2024', 'display': 'Apr-Jun', 'period': '2024Q2', 'months': '202404;202405;202406'},
            {'name': 'Q1 2024', 'display': 'Jan-Mar', 'period': '2024Q1', 'months': '202401;202402;202403'},
            {'name': 'Q4 2023', 'display': 'Oct-Dec', 'period': '2023Q4', 'months': '202310;202311;202312'},
        ]
    
    try:
        org_response = http_session.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{org_unit}",
            auth=auth,
            params={'fields': 'id,name,level,ancestors[id,name,level]'},
            timeout=30
        )
        
        if org_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch org unit info'})
        
        org_info = org_response.json()
        unit_name = org_info.get('name', 'Unknown')
        unit_level = org_info.get('level', 1)
        
        annual_population = 0
        
        if custom_population and custom_population.isdigit():
            annual_population = int(custom_population)
        else:
            if district_name:
                annual_population = UBOS_POPULATION.get(district_name, 0)
                if annual_population == 0:
                    annual_population = UBOS_POPULATION.get(clean_district_name(district_name), 0)
            
            if annual_population == 0:
                annual_population = UBOS_POPULATION.get(unit_name.upper(), 0)
                if annual_population == 0:
                    annual_population = UBOS_POPULATION.get(clean_district_name(unit_name), 0)
            
            if annual_population == 0 and unit_level >= 4:
                ancestors = org_info.get('ancestors', [])
                for ancestor in ancestors:
                    anc_name = ancestor.get('name', '').upper()
                    anc_cleaned = clean_district_name(anc_name)
                    if ancestor.get('level') == 3:
                        annual_population = UBOS_POPULATION.get(anc_name, 0)
                        if annual_population == 0:
                            annual_population = UBOS_POPULATION.get(anc_cleaned, 0)
                        if annual_population > 0:
                            break
        
        if annual_population == 0:
            return jsonify({
                'error': f'Population data not found for "{unit_name}". Please select a district or enter custom population.',
                'unit_name': unit_name,
                'quarters': [],
                'summary': {'cat1': 0, 'cat2': 0, 'cat3': 0, 'cat4': 0},
                'total_quarters': 0
            })
        
        elements_data = fetch_data_elements(auth, '105-CL')
        if 'error' in elements_data:
            return jsonify(elements_data)
        
        elements = elements_data.get('dataElements', [])
        code_to_id = {e['code']: e['id'] for e in elements}
        bcg_id = code_to_id.get('105-CL01')
        dpt1_id = code_to_id.get('105-CL10')
        dpt3_id = code_to_id.get('105-CL12')
        mr_id = code_to_id.get('105-CL23')
        
        if not all([bcg_id, dpt1_id, dpt3_id, mr_id]):
            return jsonify({'error': 'Missing required data elements for RED analysis'})
        
        dx_dimension = f"{bcg_id};{dpt1_id};{dpt3_id};{mr_id}"
        
        all_months = []
        for q in quarters:
            all_months.extend(q['months'].split(';'))
        pe_dimension = ";".join(all_months)
        
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{pe_dimension}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=120
        )
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        
        data = data_response.json()
        
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        period_data = {}
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            pe = row[pe_idx] if len(row) > pe_idx else ''
            try:
                value = int(float(row[val_idx])) if len(row) > val_idx else 0
            except:
                value = 0
            
            if pe not in period_data:
                period_data[pe] = {'bcg': 0, 'dpt1': 0, 'dpt3': 0, 'mr': 0}
            
            if dx_id == bcg_id:
                period_data[pe]['bcg'] += value
            elif dx_id == dpt1_id:
                period_data[pe]['dpt1'] += value
            elif dx_id == dpt3_id:
                period_data[pe]['dpt3'] += value
            elif dx_id == mr_id:
                period_data[pe]['mr'] += value
        
        results = []
        cat_counts = {'cat1': 0, 'cat2': 0, 'cat3': 0, 'cat4': 0}
        
        quarterly_population = round(annual_population / 4)
        target_pop = round(annual_population * 0.043 / 4)
        target_bcg = round(annual_population * 0.0485 / 4)
        
        for q in quarters:
            months = q['months'].split(';')
            
            bcg = sum(period_data.get(m, {}).get('bcg', 0) for m in months)
            dpt1 = sum(period_data.get(m, {}).get('dpt1', 0) for m in months)
            dpt3 = sum(period_data.get(m, {}).get('dpt3', 0) for m in months)
            mr = sum(period_data.get(m, {}).get('mr', 0) for m in months)
            
            bcg_cov = round((bcg / target_bcg) * 100, 1) if target_bcg > 0 else 0
            dpt1_cov = round((dpt1 / target_pop) * 100, 1) if target_pop > 0 else 0
            dpt3_cov = round((dpt3 / target_pop) * 100, 1) if target_pop > 0 else 0
            mr_cov = round((mr / target_pop) * 100, 1) if target_pop > 0 else 0
            
            unimm_dpt3 = max(0, target_pop - dpt3)
            unimm_mr = max(0, target_pop - mr)
            zero_dose = max(0, target_pop - dpt1)
            under_imm = max(0, dpt1 - dpt3)
            
            dpt1_3_dropout = round(((dpt1 - dpt3) / dpt1) * 100, 1) if dpt1 > 0 else 0
            dpt1_mr_dropout = round(((dpt1 - mr) / dpt1) * 100, 1) if dpt1 > 0 else 0
            
            access = 'Good' if dpt1_cov >= 90 else 'Poor'
            utilization = 'Good' if dpt1_3_dropout <= 10 else 'Poor'
            
            if access == 'Good' and utilization == 'Good':
                category = 'Cat. 1'
                cat_counts['cat1'] += 1
            elif access == 'Good' and utilization == 'Poor':
                category = 'Cat. 2'
                cat_counts['cat2'] += 1
            elif access == 'Poor' and utilization == 'Good':
                category = 'Cat. 3'
                cat_counts['cat3'] += 1
            else:
                category = 'Cat. 4'
                cat_counts['cat4'] += 1
            
            results.append({
                'name': q['name'],
                'display': q.get('display', q['name']),
                'period': q['period'],
                'population': quarterly_population,
                'target_pop': target_pop,
                'target_bcg': target_bcg,
                'annual_population': annual_population,
                'bcg': bcg, 'dpt1': dpt1, 'dpt3': dpt3, 'mr': mr,
                'bcg_cov': bcg_cov, 'dpt1_cov': dpt1_cov, 'dpt3_cov': dpt3_cov, 'mr_cov': mr_cov,
                'unimm_dpt3': unimm_dpt3, 'unimm_mr': unimm_mr,
                'zero_dose': zero_dose, 'under_imm': under_imm,
                'dpt1_3_dropout': dpt1_3_dropout, 'dpt1_mr_dropout': dpt1_mr_dropout,
                'access': access, 'utilization': utilization, 'category': category
            })
        
        result = {
            'unit_name': unit_name,
            'annual_population': annual_population,
            'quarterly_population': quarterly_population,
            'quarterly_target': target_pop,
            'quarters': results,
            'summary': cat_counts,
            'total_quarters': len(results),
            '_cached': False
        }
        
        analytics_cache.set(cache_key, result)
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try again'})
    except Exception as e:
        return jsonify({'error': str(e)})


@epi_bp.route('/api/trend-analysis')
@login_required
def trend_analysis():
    """Trend analysis with outlier detection and forecasting"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    indicator_id = request.args.get('indicator', '')
    period = request.args.get('period', 'LAST_24_MONTHS')
    
    if not indicator_id:
        return jsonify({'error': 'Indicator required'})
    
    if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
        start, end = period.split('-')
        periods = generate_monthly_periods(start, end)
    else:
        periods = period
    
    try:
        params = [
            ('dimension', f'dx:{indicator_id}'),
            ('dimension', f'pe:{periods}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        response = http_session.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=DHIS2_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            time_series = []
            
            for row in data.get('rows', []):
                try:
                    val = row[-1]
                    time_series.append({'period': row[1], 'value': int(float(val)) if val else 0})
                except (ValueError, IndexError):
                    continue
            
            time_series.sort(key=lambda x: x['period'])
            values = [t['value'] for t in time_series]
            
            return jsonify({
                'data': time_series,
                'outliers': detect_outliers_zscore(values, periods=[t['period'] for t in time_series]),
                'forecast': simple_forecast(values),
                'stats': {
                    'mean': round(sum(values) / len(values), 1) if values else 0,
                    'min': min(values) if values else 0,
                    'max': max(values) if values else 0,
                }
            })
        
        return jsonify({'error': f'Error: {response.status_code}'})
    except Exception as e:
        return jsonify({'error': str(e)})




