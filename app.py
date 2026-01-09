from flask import Flask, jsonify, request, render_template, session, redirect, url_for
from flask_cors import CORS
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
import json
import hashlib
from datetime import datetime, timedelta
from functools import wraps
import threading

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'epi-dashboard-secret-key-2024')
CORS(app)

# Register Blueprints
from modules.reporting import reporting_bp
from modules.maternal import maternal_bp
from modules.epi import epi_bp
from modules.wash import wash_bp
from modules.malaria import malaria_bp
app.register_blueprint(reporting_bp)
app.register_blueprint(maternal_bp)
app.register_blueprint(epi_bp)
app.register_blueprint(wash_bp)
app.register_blueprint(malaria_bp)

# ============ CACHING SYSTEM ============
class SimpleCache:
    """Thread-safe in-memory cache with expiration"""
    def __init__(self, default_ttl=300):  # 5 minutes default
        self._cache = {}
        self._lock = threading.Lock()
        self.default_ttl = default_ttl
    
    def _make_key(self, *args, **kwargs):
        """Generate cache key from args"""
        key_data = json.dumps({'args': args, 'kwargs': sorted(kwargs.items())}, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key):
        """Get item from cache if not expired"""
        with self._lock:
            if key in self._cache:
                item = self._cache[key]
                if datetime.now() < item['expires']:
                    return item['value']
                else:
                    del self._cache[key]
        return None
    
    def set(self, key, value, ttl=None):
        """Set item in cache with TTL"""
        if ttl is None:
            ttl = self.default_ttl
        with self._lock:
            self._cache[key] = {
                'value': value,
                'expires': datetime.now() + timedelta(seconds=ttl),
                'created': datetime.now()
            }
    
    def delete(self, key):
        """Remove item from cache"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    def clear(self):
        """Clear all cache"""
        with self._lock:
            self._cache.clear()
    
    def stats(self):
        """Get cache statistics"""
        with self._lock:
            now = datetime.now()
            valid = sum(1 for v in self._cache.values() if now < v['expires'])
            return {
                'total_entries': len(self._cache),
                'valid_entries': valid,
                'expired_entries': len(self._cache) - valid
            }

# Initialize cache instances
org_units_cache = SimpleCache(default_ttl=3600)      # 1 hour - org units rarely change
data_elements_cache = SimpleCache(default_ttl=3600)  # 1 hour - data elements rarely change
analytics_cache = SimpleCache(default_ttl=300)       # 5 minutes - analytics data changes
search_cache = SimpleCache(default_ttl=600)          # 10 minutes - search results

def cached(cache_instance, ttl=None):
    """Decorator for caching function results"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Generate cache key
            key = cache_instance._make_key(f.__name__, *args, **kwargs)
            
            # Try to get from cache
            result = cache_instance.get(key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = f(*args, **kwargs)
            
            # Only cache successful results
            if isinstance(result, dict) and 'error' not in result:
                cache_instance.set(key, result, ttl)
            
            return result
        return wrapper
    return decorator

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

def detect_outliers_zscore(values, periods=None, threshold=2):
    if len(values) < 3: return []
    import statistics
    ICHD_MONTHS = ['04', '10']  # ICHD campaigns in April and October
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    if std == 0: return []
    outliers = []
    for i, v in enumerate(values):
        zscore = (v - mean) / std
        is_ichd = False
        if periods and i < len(periods):
            month = str(periods[i])[4:6] if len(str(periods[i])) >= 6 else ''
            is_ichd = month in ICHD_MONTHS
        eff_threshold = threshold + 1 if is_ichd else threshold  # Higher threshold for ICHD months
        if abs(zscore) > eff_threshold:
            outliers.append({"index": i, "value": v, "zscore": round(zscore, 2), "is_ichd": is_ichd})
    return outliers

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
    return render_template('landing.html')

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

@app.route('/api/cache-stats')
def cache_stats():
    """Get cache statistics"""
    if not is_logged_in():
        return jsonify({'error': 'Not authenticated'})
    return jsonify({
        'org_units': org_units_cache.stats(),
        'data_elements': data_elements_cache.stats(),
        'analytics': analytics_cache.stats(),
        'search': search_cache.stats()
    })

@app.route('/api/clear-cache', methods=['POST'])
def clear_cache():
    """Clear all caches"""
    if not is_logged_in():
        return jsonify({'error': 'Not authenticated'})
    org_units_cache.clear()
    data_elements_cache.clear()
    analytics_cache.clear()
    search_cache.clear()
    return jsonify({'success': True, 'message': 'All caches cleared'})

def fetch_org_units_cached(auth, parent_id=None):
    """Fetch org units with caching"""
    cache_key = org_units_cache._make_key('org_units', parent_id)
    cached = org_units_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        if parent_id:
            response = requests.get(f"{DHIS2_BASE_URL}/organisationUnits/{parent_id}",
                auth=auth, params={'fields': 'id,displayName,children[id,displayName,level,childCount]'}, timeout=30)
        else:
            response = requests.get(f"{DHIS2_BASE_URL}/organisationUnits",
                auth=auth, params={'level': 1, 'fields': 'id,displayName,level,childCount', 'paging': 'false'}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            org_units_cache.set(cache_key, data)
            return data
        return {'error': f'Status {response.status_code}'}
    except requests.exceptions.Timeout:
        return {'error': 'Connection timeout - try again'}
    except Exception as e:
        return {'error': str(e)}

@app.route('/api/org-units')
def get_org_units():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    parent_id = request.args.get('parent')
    result = fetch_org_units_cached(auth, parent_id)
    return jsonify(result)

@app.route('/api/org-units/<string:org_unit_id>')
def get_org_unit_details(org_unit_id):
    """Get a single organization unit with its ancestors"""
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'}), 401
    
    cache_key = search_cache._make_key('org_unit_details', org_unit_id)
    cached = search_cache.get(cache_key)
    if cached:
        return jsonify(cached)

    try:
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{org_unit_id}",
            auth=auth,
            params={'fields': 'id,displayName,level,ancestors[id,displayName,level]'},
            timeout=30
        )
        response.raise_for_status()
        org_unit = response.json()
        search_cache.set(cache_key, org_unit, ttl=3600)  # Cache for 1 hour
        return jsonify(org_unit)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching org unit details for {org_unit_id}: {e}")
        return jsonify({'error': 'Failed to fetch organization unit details', 'details': str(e)}), 500

@app.route('/api/districts')
def get_districts():
    return jsonify(UBOS_POPULATION)

@app.route('/api/search-org-units')
def search_org_units():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    query = request.args.get('query', '').strip().lower()
    if len(query) < 3:
        return jsonify({'error': 'Query must be at least 3 characters'})
    
    # Check cache first
    cache_key = search_cache._make_key('search_org', query)
    cached = search_cache.get(cache_key)
    if cached:
        return jsonify(cached)
    
    try:
        # Search for org units by name
        response = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits",
            auth=auth,
            params={
                'filter': f'displayName:ilike:{query}',
                'fields': 'id,displayName,level,path,parent[id,displayName]',
                'paging': 'false'
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            units = data.get('organisationUnits', [])
            
            # Format results with readable path
            for unit in units:
                if 'path' in unit:
                    parent = unit.get('parent', {})
                    unit['path'] = parent.get('displayName', '')
                else:
                    unit['path'] = ''
            
            # Sort by level (districts first), then by name
            units.sort(key=lambda x: (x.get('level', 99), x.get('displayName', '')))
            
            result = {'organisationUnits': units}
            search_cache.set(cache_key, result)
            return jsonify(result)
        
        return jsonify({'error': f'Search failed: {response.status_code}'})
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Search timeout - try again'})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/org-units-descendants')
def get_org_units_descendants():
    """
    Fetch descendant org units under a parent, optionally filtered by level.

    Use-case: list all facilities (level 6) within a selected district (level 3),
    without manually drilling down county/sub-county.
    """
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'}), 401

    parent_id = request.args.get('parent')
    level = request.args.get('level', type=int)  # e.g. 6 for facilities

    if not parent_id:
        return jsonify({'error': 'parent is required'}), 400

    cache_key = org_units_cache._make_key('org_units_descendants', parent_id, level or '')
    cached = org_units_cache.get(cache_key)
    if cached:
        return jsonify(cached)

    try:
        # Get parent path first
        parent_resp = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{parent_id}",
            auth=auth,
            params={'fields': 'id,displayName,path,level'},
            timeout=30
        )
        if parent_resp.status_code != 200:
            return jsonify({'error': 'Failed to fetch parent org unit', 'status': parent_resp.status_code}), 502

        parent = parent_resp.json()
        parent_path = parent.get('path')
        if not parent_path:
            return jsonify({'error': 'Parent org unit has no path (unexpected)', 'parent': parent}), 502

        params = {
            'fields': 'id,displayName,level,parent[id,displayName]',
            'paging': 'false',
        }

        # DHIS2 filter syntax supports multiple filter params.
        # Example: filter=path:like:/...&filter=level:eq:6
        filters = [f"path:like:{parent_path}"]
        if level:
            filters.append(f"level:eq:{level}")
        params['filter'] = filters

        resp = requests.get(
            f"{DHIS2_BASE_URL}/organisationUnits",
            auth=auth,
            params=params,
            timeout=60
        )

        if resp.status_code != 200:
            return jsonify({'error': 'Failed to fetch descendants', 'status': resp.status_code, 'details': resp.text[:200]}), 502

        data = resp.json()
        # Normalise return shape for frontend consumption
        result = {
            'parent': {'id': parent.get('id'), 'displayName': parent.get('displayName'), 'level': parent.get('level')},
            'organisationUnits': data.get('organisationUnits', []),
        }
        org_units_cache.set(cache_key, result, ttl=3600)
        return jsonify(result)
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Connection timeout - try again'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def fetch_data_elements_cached(auth, pattern='105-CL'):
    """Fetch data elements with caching"""
    cache_key = data_elements_cache._make_key('data_elements', pattern)
    cached = data_elements_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        response = requests.get(f"{DHIS2_BASE_URL}/dataElements",
            auth=auth, params={'filter': f'code:like:{pattern}', 'fields': 'id,code,displayName,shortName', 'paging': 'false'}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            data_elements_cache.set(cache_key, data)
            return data
        return {'error': f'Status {response.status_code}'}
    except requests.exceptions.Timeout:
        return {'error': 'Connection timeout'}
    except Exception as e:
        return {'error': str(e)}

@app.route('/api/search-data-elements')
def search_data_elements():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    pattern = request.args.get('pattern', '105-CL')
    result = fetch_data_elements_cached(auth, pattern)
    return jsonify(result)

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
    
    # Check cache
    cache_key = analytics_cache._make_key('raw_data', org_unit, period, indicators)
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    try:
        # Get data elements (cached)
        elements_data = fetch_data_elements_cached(auth, '105-CL')
        if 'error' in elements_data:
            return jsonify(elements_data)
        
        elements = elements_data.get('dataElements', [])
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
                data['_cached'] = False
                analytics_cache.set(cache_key, data)
                return jsonify(data)
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        return jsonify({'error': 'No data elements found'})
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try again or select a smaller time period'})
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
    custom_population = request.args.get('customPopulation', None)
    
    # Check cache first (include custom population in cache key if provided)
    cache_key = analytics_cache._make_key('analytics_data', org_unit, district_name, period, custom_population or '')
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    divisor = get_period_divisor(period)
    
    # Use custom population if provided (for facilities), otherwise use UBOS data
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
        # Get data elements (cached)
        elements_data = fetch_data_elements_cached(auth, '105-CL')
        if 'error' in elements_data:
            return jsonify(elements_data)
        
        if True:  # Replaces: if response.status_code == 200:
            elements = elements_data.get('dataElements', [])
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
                
                analytics_result['_cached'] = False
                analytics_cache.set(cache_key, analytics_result)
                return jsonify(analytics_result)
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        return jsonify({'error': 'Failed to fetch data'})
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout - try again or select a smaller time period'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/red-categorization')
def red_categorization():
    """RED Categorization Tool - Analyze selected unit across quarters for progress monitoring"""
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    custom_population = request.args.get('customPopulation', None)
    
    # Get custom date range if provided
    start_date = request.args.get('startDate', None)  # Format: YYYY-MM
    end_date = request.args.get('endDate', None)      # Format: YYYY-MM
    
    # Check cache
    cache_key = analytics_cache._make_key('red_categorization_quarterly', org_unit, custom_population or '', start_date or '', end_date or '')
    cached = analytics_cache.get(cache_key)
    if cached:
        cached['_cached'] = True
        return jsonify(cached)
    
    from datetime import datetime
    from calendar import month_abbr
    
    # Helper function to generate quarters from date range
    def generate_quarters(start, end):
        """Generate quarters between start and end dates"""
        quarters = []
        
        # Parse dates
        start_y, start_m = map(int, start.split('-'))
        end_y, end_m = map(int, end.split('-'))
        
        # Generate all months in range
        current_y, current_m = start_y, start_m
        months_list = []
        
        while (current_y < end_y) or (current_y == end_y and current_m <= end_m):
            months_list.append((current_y, current_m))
            current_m += 1
            if current_m > 12:
                current_m = 1
                current_y += 1
        
        # Group into quarters
        i = 0
        quarter_num = 1
        while i < len(months_list):
            # Take up to 3 months for a quarter
            quarter_months = months_list[i:i+3]
            if not quarter_months:
                break
            
            month_codes = [f"{y}{m:02d}" for y, m in quarter_months]
            month_names = [month_abbr[m] for y, m in quarter_months]
            
            # Create quarter name
            if len(quarter_months) == 3:
                q_name = f"Q{quarter_num} {quarter_months[0][0]}"
                q_display = f"{month_names[0]}-{month_names[2]}"
            else:
                # Partial quarter
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
    
    # Generate quarters based on input
    if start_date and end_date:
        try:
            quarters = generate_quarters(start_date, end_date)
            if not quarters:
                return jsonify({'error': 'Invalid date range'})
        except:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM'})
    else:
        # Default: Last 8 quarters ending at Q3 2025 (July-September 2025)
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
        # Get org unit info including ancestors for district lookup
        org_response = requests.get(
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
        
        # Get population - try multiple sources
        annual_population = 0
        
        # Helper function to clean district name for UBOS lookup
        def clean_district_name(name):
            """Remove common suffixes like 'District', 'City', etc. for UBOS lookup"""
            cleaned = name.upper().strip()
            for suffix in [' DISTRICT', ' CITY', ' MUNICIPALITY', ' TOWN COUNCIL', ' SUB COUNTY', ' SUBCOUNTY', ' PARISH', ' HC II', ' HC III', ' HC IV', ' HOSPITAL']:
                if cleaned.endswith(suffix):
                    cleaned = cleaned[:-len(suffix)].strip()
            return cleaned
        
        if custom_population and custom_population.isdigit():
            annual_population = int(custom_population)
        else:
            # Try direct lookup with provided district name
            if district_name:
                annual_population = UBOS_POPULATION.get(district_name, 0)
                # Try cleaned version
                if annual_population == 0:
                    annual_population = UBOS_POPULATION.get(clean_district_name(district_name), 0)
            
            # If not found, try unit name directly
            if annual_population == 0:
                annual_population = UBOS_POPULATION.get(unit_name.upper(), 0)
                # Try cleaned version
                if annual_population == 0:
                    annual_population = UBOS_POPULATION.get(clean_district_name(unit_name), 0)
            
            # If still not found and this is a sub-district unit, look up parent district
            if annual_population == 0 and unit_level >= 4:
                ancestors = org_info.get('ancestors', [])
                # Find district level ancestor (usually level 3)
                for ancestor in ancestors:
                    anc_name = ancestor.get('name', '').upper()
                    anc_cleaned = clean_district_name(anc_name)
                    if ancestor.get('level') == 3:  # District level
                        annual_population = UBOS_POPULATION.get(anc_name, 0)
                        if annual_population == 0:
                            annual_population = UBOS_POPULATION.get(anc_cleaned, 0)
                        if annual_population > 0:
                            break
        
        # If still no population found, return error with helpful message
        if annual_population == 0:
            return jsonify({
                'error': f'Population data not found for "{unit_name}". Please select a district or enter custom population for facilities.',
                'unit_name': unit_name,
                'quarters': [],
                'summary': {'cat1': 0, 'cat2': 0, 'cat3': 0, 'cat4': 0},
                'total_quarters': 0
            })
        
        # Relevant data element codes for RED analysis
        elements_data = fetch_data_elements_cached(auth, '105-CL')
        if 'error' in elements_data:
            return jsonify(elements_data)
        
        elements = elements_data.get('dataElements', [])
        
        # Find relevant element IDs
        code_to_id = {e['code']: e['id'] for e in elements}
        bcg_id = code_to_id.get('105-CL01')
        dpt1_id = code_to_id.get('105-CL10')
        dpt3_id = code_to_id.get('105-CL12')
        mr_id = code_to_id.get('105-CL23')
        
        if not all([bcg_id, dpt1_id, dpt3_id, mr_id]):
            return jsonify({'error': 'Missing required data elements for RED analysis'})
        
        dx_dimension = f"{bcg_id};{dpt1_id};{dpt3_id};{mr_id}"
        
        # Collect all periods for a single query
        all_months = []
        for q in quarters:
            all_months.extend(q['months'].split(';'))
        pe_dimension = ";".join(all_months)
        
        # Fetch analytics data for all periods at once
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{pe_dimension}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = requests.get(f"{DHIS2_BASE_URL}/analytics", auth=auth, params=params, timeout=120)
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'})
        
        data = data_response.json()
        
        # Parse results by period
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        # Aggregate by period and data element
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
        
        # Aggregate by quarter
        results = []
        cat_counts = {'cat1': 0, 'cat2': 0, 'cat3': 0, 'cat4': 0}
        
        # Quarterly calculations based on annual population
        # TOTAL (Quarterly Population) = Annual Population / 4
        # Q-TARGET = (Annual Population × 4.3%) / 4
        # Q-TARGET BCG = (Annual Population × 4.85%) / 4
        quarterly_population = round(annual_population / 4)  # For display
        target_pop = round(annual_population * 0.043 / 4)    # Q-TARGET for DPT/MR
        target_bcg = round(annual_population * 0.0485 / 4)   # Q-TARGET for BCG
        
        for q in quarters:
            months = q['months'].split(';')
            
            # Sum doses for this quarter
            bcg = sum(period_data.get(m, {}).get('bcg', 0) for m in months)
            dpt1 = sum(period_data.get(m, {}).get('dpt1', 0) for m in months)
            dpt3 = sum(period_data.get(m, {}).get('dpt3', 0) for m in months)
            mr = sum(period_data.get(m, {}).get('mr', 0) for m in months)
            
            # Calculate coverages
            bcg_cov = round((bcg / target_bcg) * 100, 1) if target_bcg > 0 else 0
            dpt1_cov = round((dpt1 / target_pop) * 100, 1) if target_pop > 0 else 0
            dpt3_cov = round((dpt3 / target_pop) * 100, 1) if target_pop > 0 else 0
            mr_cov = round((mr / target_pop) * 100, 1) if target_pop > 0 else 0
            
            # Calculate unimmunized
            unimm_dpt3 = max(0, target_pop - dpt3)
            unimm_mr = max(0, target_pop - mr)
            zero_dose = max(0, target_pop - dpt1)
            under_imm = max(0, dpt1 - dpt3)
            
            # Calculate dropout rates
            dpt1_3_dropout = round(((dpt1 - dpt3) / dpt1) * 100, 1) if dpt1 > 0 else 0
            dpt1_mr_dropout = round(((dpt1 - mr) / dpt1) * 100, 1) if dpt1 > 0 else 0
            
            # Determine Access and Utilization
            access = 'Good' if dpt1_cov >= 90 else 'Poor'
            utilization = 'Good' if dpt1_3_dropout <= 10 else 'Poor'
            
            # Determine Category
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
                'display': q.get('display', q['name']),  # Month names (e.g., "Jul-Sep")
                'period': q['period'],
                'population': quarterly_population,  # TOTAL = Annual / 4
                'target_pop': target_pop,            # Q-TARGET = Annual × 4.3% / 4
                'target_bcg': target_bcg,            # Q-TARGET BCG = Annual × 4.85% / 4
                'annual_population': annual_population,  # For reference
                'bcg': bcg,
                'dpt1': dpt1,
                'dpt3': dpt3,
                'mr': mr,
                'bcg_cov': bcg_cov,
                'dpt1_cov': dpt1_cov,
                'dpt3_cov': dpt3_cov,
                'mr_cov': mr_cov,
                'unimm_dpt3': unimm_dpt3,
                'unimm_mr': unimm_mr,
                'zero_dose': zero_dose,
                'under_imm': under_imm,
                'dpt1_3_dropout': dpt1_3_dropout,
                'dpt1_mr_dropout': dpt1_mr_dropout,
                'access': access,
                'utilization': utilization,
                'category': category
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
        return jsonify({'error': 'Request timeout - try again or select a smaller time period'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/trend-analysis')
def trend_analysis():
    auth = get_auth()
    if not auth:
        return jsonify({'error': 'Not authenticated'})
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    indicator_id = request.args.get('indicator', '')
    period = request.args.get('period', 'LAST_24_MONTHS')  # 24 months for full seasonal view
    
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
                try:
                    val = row[-1]  # Value is typically the last column
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
