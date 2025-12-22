from flask import Flask, jsonify, request, render_template, session, redirect, url_for
from flask_cors import CORS
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
import json

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'epi-dashboard-secret-key-2024')
CORS(app)

DHIS2_BASE_URL = 'https://hmis.health.go.ug/api'

# UBOS Population Data (Annual figures)
UBOS_POPULATION = {
    "ABIM": 144084, "ADJUMANI": 297894, "AGAGO": 307235, "ALEBTONG": 283509,
    "AMOLATAR": 188715, "AMUDAT": 203358, "AMURIA": 251653, "AMURU": 240814,
    "APAC": 221962, "ARUA": 159722, "ARUA CITY": 384656, "BUDAKA": 281537,
    "BUDUDA": 268970, "BUGIRI": 480345, "BUGWERI": 211511, "BUHWEJU": 167921,
    "BUIKWE": 520158, "BUKEDEA": 282864, "BUKOMANSIMBI": 197568, "BUKWO": 114396,
    "BULAMBULI": 235391, "BULIISA": 167894, "BUNDIBUGYO": 264778, "BUNYANGABU": 219012,
    "BUSHENYI": 283392, "BUSIA": 412671, "BUTALEJA": 312771, "BUTAMBALA": 146516,
    "BUTEBO": 171433, "BUVUMA": 110832, "BUYENDE": 403486, "DOKOLO": 215625,
    "FORT PORTAL CITY": 137549, "GOMBA": 199120, "GULU": 135373, "GULU CITY": 233271,
    "HOIMA": 257544, "HOIMA CITY": 143304, "IBANDA": 309466, "IGANGA": 426958,
    "ISINGIRO": 635077, "JINJA": 280905, "JINJA CITY": 279184, "KAABONG": 264631,
    "KABALE": 285588, "KABAROLE": 230368, "KABERAMAIDO": 140986, "KAGADI": 471111,
    "KAKUMIRO": 428176, "KALAKI": 149736, "KALANGALA": 74411, "KALIRO": 286397,
    "KALUNGU": 221569, "KAMPALA": 1797722, "KAMULI": 540252, "KAMWENGE": 337167,
    "KANUNGU": 310062, "KAPCHORWA": 133621, "KAPELEBYONG": 143536, "KARENGA": 100375,
    "KASESE": 853831, "KASSANDA": 314008, "KATAKWI": 234332, "KAYUNGA": 439175,
    "KAZO": 208898, "KIBAALE": 237649, "KIBOGA": 183255, "KIBUKU": 249441,
    "KIKUUBE": 379547, "KIRUHURA": 203502, "KIRYANDONGO": 364872, "KISORO": 433662,
    "KITAGWENDA": 184947, "KITGUM": 239655, "KOBOKO": 271781, "KOLE": 294301,
    "KOTIDO": 219734, "KUMI": 286992, "KWANIA": 216125, "KWEEN": 129277,
    "KYANKWANZI": 278432, "KYEGEGWA": 501120, "KYENJOJO": 543998, "KYOTERA": 275917,
    "LAMWO": 213156, "LIRA": 242216, "LIRA CITY": 245132, "LUUKA": 298639,
    "LUWEERO": 616242, "LWENGO": 325263, "LYANTONDE": 133017, "MADI-OKOLLO": 178051,
    "MANAFWA": 186917, "MARACHA": 234712, "MASAKA": 115455, "MASAKA CITY": 294166,
    "MASINDI": 342635, "MAYUGE": 577563, "MBALE": 290356, "MBALE CITY": 290414,
    "MBARARA": 174039, "MBARARA CITY": 264425, "MITOOMA": 226009, "MITYANA": 407386,
    "MOROTO": 103639, "MOYO": 109572, "MPIGI": 326690, "MUBENDE": 522015,
    "MUKONO": 929224, "NABILATUK": 136785, "NAKAPIRIPIRIT": 111681, "NAKASEKE": 251398,
    "NAKASONGOLA": 226074, "NAMAYINGO": 266716, "NAMISINDWA": 257346, "NAMUTUMBA": 311339,
    "NAPAK": 211830, "NEBBI": 299398, "NGORA": 213777, "NTOROKO": 114858,
    "NTUNGAMO": 552786, "NWOYA": 220593, "OBONGI": 142983, "OMORO": 207339,
    "OTUKE": 161069, "OYAM": 477464, "PADER": 240159, "PAKWACH": 206961,
    "PALLISA": 334697, "RAKAI": 346885, "RUBANDA": 249454, "RUBIRIZI": 168211,
    "RUKIGA": 132355, "RUKUNGIRI": 376110, "RWAMPARA": 162967, "SERERE": 358123,
    "SHEEMA": 252275, "SIRONKO": 298363, "SOROTI": 266189, "SOROTI CITY": 134199,
    "SSEMBABULE": 305971, "TEREGO": 323253, "TORORO": 609939, "WAKISO": 3411177,
    "YUMBE": 945100, "ZOMBO": 312621
}

# Target population percentages
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

def get_auth():
    """Get auth from session"""
    if 'username' in session and 'password' in session:
        return HTTPBasicAuth(session['username'], session['password'])
    return None

def is_logged_in():
    return 'username' in session and 'password' in session

def get_period_divisor(period_type):
    if period_type in ['THIS_MONTH', 'LAST_MONTH'] or (len(period_type) == 6 and period_type.isdigit()):
        return 12
    elif 'QUARTER' in period_type or (len(period_type) == 6 and 'Q' in period_type):
        return 4
    return 1

def calculate_coverage(doses, population, target_pct, divisor=1):
    if population <= 0 or target_pct <= 0:
        return 0
    target_pop = (population * target_pct / 100) / divisor
    return round((doses / target_pop) * 100, 1) if target_pop > 0 else 0

def get_coverage_color(coverage):
    if coverage >= 95: return "green"
    elif coverage >= 70: return "yellow"
    return "red"

def calculate_dropout(first_dose, last_dose):
    if first_dose <= 0: return 0
    return round(((first_dose - last_dose) / first_dose) * 100, 1)

def detect_outliers_zscore(values, threshold=2):
    if len(values) < 3: return []
    import statistics
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    if std == 0: return []
    return [{"index": i, "value": v, "zscore": round((v - mean) / std, 2)} 
            for i, v in enumerate(values) if abs((v - mean) / std) > threshold]

def simple_forecast(values, periods_ahead=3):
    if len(values) < 2: return []
    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator != 0 else 0
    intercept = y_mean - slope * x_mean
    return [round(slope * (n + i) + intercept, 0) for i in range(periods_ahead)]

def generate_monthly_periods(start, end):
    periods = []
    start_year, start_month = int(start[:4]), int(start[4:6])
    end_year, end_month = int(end[:4]), int(end[4:6])
    y, m = start_year, start_month
    while y < end_year or (y == end_year and m <= end_month):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12: m, y = 1, y + 1
    return ";".join(periods)

# Routes
@app.route('/')
def index():
    if not is_logged_in():
        return redirect(url_for('login'))
    return render_template('dashboard.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json() if request.is_json else request.form
        username = data.get('username', '')
        password = data.get('password', '')
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'Username and password required'})
        
        # Test credentials against DHIS2
        try:
            response = requests.get(
                f"{DHIS2_BASE_URL}/me",
                auth=HTTPBasicAuth(username, password),
                params={'fields': 'id,displayName'},
                timeout=15
            )
            if response.status_code == 200:
                user_data = response.json()
                session['username'] = username
                session['password'] = password
                session['display_name'] = user_data.get('displayName', username)
                return jsonify({'success': True, 'displayName': session['display_name']})
            else:
                return jsonify({'success': False, 'error': 'Invalid credentials'})
        except requests.exceptions.Timeout:
            return jsonify({'success': False, 'error': 'Connection timeout - try again'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/api/check-auth')
def check_auth():
    if is_logged_in():
        return jsonify({'authenticated': True, 'displayName': session.get('display_name', 'User')})
    return jsonify({'authenticated': False})

@app.route('/api/user-info')
def get_user_info():
    if not is_logged_in():
        return jsonify({'error': 'Not authenticated'})
    return jsonify({'displayName': session.get('display_name', 'User')})

@app.route('/api/org-units')
def get_org_units():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    parent_id = request.args.get('parent')
    try:
        if parent_id:
            response = requests.get(f"{DHIS2_BASE_URL}/organisationUnits/{parent_id}",
                auth=auth, params={'fields': 'id,displayName,children[id,displayName,level,childCount]'}, timeout=30)
        else:
            response = requests.get(f"{DHIS2_BASE_URL}/organisationUnits",
                auth=auth, params={'level': 1, 'fields': 'id,displayName,level,childCount', 'paging': 'false'}, timeout=30)
        return jsonify(response.json()) if response.status_code == 200 else jsonify({'error': f'Status {response.status_code}'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/districts')
def get_districts():
    return jsonify(UBOS_POPULATION)

@app.route('/api/search-data-elements')
def search_data_elements():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    pattern = request.args.get('pattern', '105-CL')
    try:
        response = requests.get(f"{DHIS2_BASE_URL}/dataElements",
            auth=auth, params={'filter': f'code:like:{pattern}', 'fields': 'id,code,displayName,shortName', 'paging': 'false'}, timeout=30)
        return jsonify(response.json()) if response.status_code == 200 else jsonify({'error': f'Status {response.status_code}'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/raw-data')
def get_raw_data():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    period = request.args.get('period', 'LAST_12_MONTHS')
    indicators = request.args.get('indicators', '')
    
    if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
        start, end = period.split('-')
        periods = generate_monthly_periods(start, end)
    else:
        periods = period
    
    try:
        response = requests.get(f"{DHIS2_BASE_URL}/dataElements",
            auth=auth, params={'filter': 'code:like:105-CL', 'fields': 'id,code,displayName', 'paging': 'false'}, timeout=30)
        
        if response.status_code == 200:
            elements = response.json().get('dataElements', [])
            if indicators:
                ids = [i.strip() for i in indicators.split(',') if i.strip()]
            else:
                ids = [e['id'] for e in elements]
            
            if ids:
                dx_dimension = ";".join(ids)
                params = [('dimension', f'dx:{dx_dimension}'), ('dimension', f'pe:{periods}'),
                          ('dimension', f'ou:{org_unit}'), ('displayProperty', 'NAME'), ('skipMeta', 'false')]
                data_response = requests.get(f"{DHIS2_BASE_URL}/analytics", auth=auth, params=params, timeout=60)
                if data_response.status_code == 200:
                    data = data_response.json()
                    data['dataElementMeta'] = {e['id']: e for e in elements}
                    return jsonify(data)
                return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        return jsonify({'error': 'No data elements found'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/analytics-data')
def get_analytics_data():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    period = request.args.get('period', 'LAST_12_MONTHS')
    
    divisor = get_period_divisor(period)
    population = UBOS_POPULATION.get(district_name, 0)
    
    if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
        start, end = period.split('-')
        periods = generate_monthly_periods(start, end)
        period_count = len(periods.split(';'))
        divisor = 12 / period_count if period_count < 12 else 1
    else:
        periods = period
    
    try:
        response = requests.get(f"{DHIS2_BASE_URL}/dataElements",
            auth=auth, params={'filter': 'code:like:105-CL', 'fields': 'id,code,displayName,shortName', 'paging': 'false'}, timeout=30)
        
        if response.status_code == 200:
            elements = response.json().get('dataElements', [])
            ids = [e['id'] for e in elements]
            code_map = {e['id']: e['code'] for e in elements}
            
            dx_dimension = ";".join(ids)
            params = [('dimension', f'dx:{dx_dimension}'), ('dimension', f'pe:{periods}'),
                      ('dimension', f'ou:{org_unit}'), ('displayProperty', 'NAME'), ('skipMeta', 'false')]
            data_response = requests.get(f"{DHIS2_BASE_URL}/analytics", auth=auth, params=params, timeout=60)
            
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
                        'doses': total, 'target_population': round((population * target_pct / 100) / divisor),
                        'coverage': coverage, 'color': get_coverage_color(coverage)
                    })
                
                for config in DROPOUT_CONFIGS:
                    first_id = next((e['id'] for e in elements if e['code'] == config['first']), None)
                    last_id = next((e['id'] for e in elements if e['code'] == config['last']), None)
                    if first_id and last_id:
                        first_doses = indicator_totals.get(first_id, 0)
                        last_doses = indicator_totals.get(last_id, 0)
                        dropout = calculate_dropout(first_doses, last_doses)
                        analytics_result['dropouts'].append({
                            'name': config['name'], 'first_doses': first_doses,
                            'last_doses': last_doses, 'dropout_rate': dropout,
                            'color': 'red' if dropout >= 10 else 'green'
                        })
                
                return jsonify(analytics_result)
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        return jsonify({'error': 'Failed to fetch data'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/trend-analysis')
def trend_analysis():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    indicator_id = request.args.get('indicator', '')
    period = request.args.get('period', 'LAST_12_MONTHS')
    
    if not indicator_id:
        return jsonify({'error': 'Indicator required'})
    
    if '-' in period and not period.startswith('LAST') and not period.startswith('THIS'):
        start, end = period.split('-')
        periods = generate_monthly_periods(start, end)
    else:
        periods = period
    
    try:
        params = [('dimension', f'dx:{indicator_id}'), ('dimension', f'pe:{periods}'),
                  ('dimension', f'ou:{org_unit}'), ('displayProperty', 'NAME'), ('skipMeta', 'false')]
        response = requests.get(f"{DHIS2_BASE_URL}/analytics", auth=auth, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            time_series = []
            for row in data.get('rows', []):
                time_series.append({'period': row[1], 'value': int(float(row[2])) if len(row) > 2 else 0})
            
            time_series.sort(key=lambda x: x['period'])
            values = [t['value'] for t in time_series]
            
            return jsonify({
                'data': time_series,
                'outliers': detect_outliers_zscore(values),
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
