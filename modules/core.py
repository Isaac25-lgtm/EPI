"""
Core utilities shared across all health modules
- Caching system (Redis-ready for 10,000 users)
- Authentication helpers
- DHIS2 API helpers
- Population data
"""
import os
import json
import hashlib
import threading
from datetime import datetime, timedelta
from functools import wraps
from flask import session
from requests.auth import HTTPBasicAuth
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============ CONNECTION POOLING FOR SCALE ============
def create_session():
    """Create a requests session with connection pooling and retries"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=retry_strategy
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Global session for connection pooling
http_session = create_session()


# ============ CACHING SYSTEM ============
class SimpleCache:
    """Thread-safe in-memory cache with expiration
    For production with 10,000+ users, use Redis instead
    """
    def __init__(self, default_ttl=300):
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


# Initialize cache instances with appropriate TTLs
org_units_cache = SimpleCache(default_ttl=3600)      # 1 hour
data_elements_cache = SimpleCache(default_ttl=3600)  # 1 hour
analytics_cache = SimpleCache(default_ttl=300)       # 5 minutes
search_cache = SimpleCache(default_ttl=600)          # 10 minutes


def cached(cache_instance, ttl=None):
    """Decorator for caching function results"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = cache_instance._make_key(f.__name__, *args, **kwargs)
            result = cache_instance.get(key)
            if result is not None:
                return result
            result = f(*args, **kwargs)
            if isinstance(result, dict) and 'error' not in result:
                cache_instance.set(key, result, ttl)
            return result
        return wrapper
    return decorator


# ============ DHIS2 CONFIGURATION ============
DHIS2_BASE_URL = os.getenv('DHIS2_BASE_URL', 'https://hmis.health.go.ug/api')
DHIS2_TIMEOUT = int(os.getenv('DHIS2_TIMEOUT', '60'))


# ============ AUTHENTICATION HELPERS ============
def get_auth():
    """Get auth from session"""
    if 'username' in session and 'password' in session:
        return HTTPBasicAuth(session['username'], session['password'])
    return None


def is_logged_in():
    """Check if user is logged in"""
    return 'username' in session and 'password' in session


def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            from flask import jsonify
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return wrapper


# ============ UBOS POPULATION DATA ============
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


# ============ HELPER FUNCTIONS ============
def get_period_divisor(period_type):
    """Calculate period divisor for annualized rates"""
    if period_type in ['THIS_MONTH', 'LAST_MONTH'] or (len(period_type) == 6 and period_type.isdigit()):
        return 12
    elif 'QUARTER' in period_type or (len(period_type) == 6 and 'Q' in period_type):
        return 4
    return 1


def calculate_coverage(doses, population, target_pct, divisor=1):
    """Calculate coverage percentage"""
    if population <= 0 or target_pct <= 0:
        return 0
    target_pop = (population * target_pct / 100) / divisor
    return round((doses / target_pop) * 100, 1) if target_pop > 0 else 0


def get_coverage_color(coverage):
    """Get color code for coverage level"""
    if coverage >= 95: return "green"
    elif coverage >= 70: return "yellow"
    return "red"


def calculate_dropout(first_dose, last_dose):
    """Calculate dropout rate between doses"""
    if first_dose <= 0: return 0
    return round(((first_dose - last_dose) / first_dose) * 100, 1)


def generate_monthly_periods(start, end):
    """Generate monthly period string from date range"""
    periods = []
    start_year, start_month = int(start[:4]), int(start[4:6])
    end_year, end_month = int(end[:4]), int(end[4:6])
    y, m = start_year, start_month
    while y < end_year or (y == end_year and m <= end_month):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12: m, y = 1, y + 1
    return ";".join(periods)


def detect_outliers_zscore(values, periods=None, threshold=2):
    """Detect outliers using z-score method"""
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
        eff_threshold = threshold + 1 if is_ichd else threshold
        if abs(zscore) > eff_threshold:
            outliers.append({"index": i, "value": v, "zscore": round(zscore, 2), "is_ichd": is_ichd})
    return outliers


def simple_forecast(values, periods_ahead=3):
    """Simple linear regression forecast"""
    if len(values) < 2: return []
    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator != 0 else 0
    intercept = y_mean - slope * x_mean
    return [round(slope * (n + i) + intercept, 0) for i in range(periods_ahead)]


def clean_district_name(name):
    """Remove common suffixes for UBOS lookup"""
    cleaned = name.upper().strip()
    for suffix in [' DISTRICT', ' CITY', ' MUNICIPALITY', ' TOWN COUNCIL', ' SUB COUNTY', 
                   ' SUBCOUNTY', ' PARISH', ' HC II', ' HC III', ' HC IV', ' HOSPITAL']:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
    return cleaned


# ============ DHIS2 API HELPERS ============
def fetch_org_units(auth, parent_id=None):
    """Fetch org units with caching"""
    cache_key = org_units_cache._make_key('org_units', parent_id)
    cached = org_units_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        if parent_id:
            response = http_session.get(
                f"{DHIS2_BASE_URL}/organisationUnits/{parent_id}",
                auth=auth,
                params={'fields': 'id,displayName,children[id,displayName,level,childCount]'},
                timeout=DHIS2_TIMEOUT
            )
        else:
            response = http_session.get(
                f"{DHIS2_BASE_URL}/organisationUnits",
                auth=auth,
                params={'level': 1, 'fields': 'id,displayName,level,childCount', 'paging': 'false'},
                timeout=DHIS2_TIMEOUT
            )
        
        if response.status_code == 200:
            data = response.json()
            org_units_cache.set(cache_key, data)
            return data
        return {'error': f'Status {response.status_code}'}
    except requests.exceptions.Timeout:
        return {'error': 'Connection timeout - try again'}
    except Exception as e:
        return {'error': str(e)}


def fetch_data_elements(auth, pattern='105-CL'):
    """Fetch data elements with caching"""
    cache_key = data_elements_cache._make_key('data_elements', pattern)
    cached = data_elements_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        response = http_session.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={'filter': f'code:like:{pattern}', 'fields': 'id,code,displayName,shortName', 'paging': 'false'},
            timeout=DHIS2_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            data_elements_cache.set(cache_key, data)
            return data
        return {'error': f'Status {response.status_code}'}
    except requests.exceptions.Timeout:
        return {'error': 'Connection timeout'}
    except Exception as e:
        return {'error': str(e)}

