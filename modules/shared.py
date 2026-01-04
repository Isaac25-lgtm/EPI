"""
Shared Utilities Module
=======================
Common functions, constants, and utilities used across all health modules.
"""

from flask import session
from requests.auth import HTTPBasicAuth
from functools import wraps
from flask import jsonify
import json
import hashlib
from datetime import datetime, timedelta
import threading

# ============ DHIS2 CONFIGURATION ============

DHIS2_BASE_URL = 'https://hmis.health.go.ug/api'

# ============ UBOS POPULATION DATA ============
# Annual population figures for 146 districts in Uganda

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


# Global cache instances
org_units_cache = SimpleCache(default_ttl=3600)      # 1 hour
data_elements_cache = SimpleCache(default_ttl=3600)  # 1 hour
analytics_cache = SimpleCache(default_ttl=300)       # 5 minutes
search_cache = SimpleCache(default_ttl=600)          # 10 minutes


# ============ AUTHENTICATION HELPERS ============

def get_auth():
    """Get authentication from session"""
    if 'username' in session and 'password' in session:
        return HTTPBasicAuth(session['username'], session['password'])
    return None

def is_logged_in():
    """Check if user is logged in"""
    return 'username' in session and 'password' in session

def login_required(f):
    """Decorator to require login for API endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated_function


# ============ CALCULATION HELPERS ============

def get_period_divisor(period_type):
    """
    Get divisor based on period type for annualizing data
    Monthly = 12, Quarterly = 4, Annual = 1
    """
    if period_type in ['THIS_MONTH', 'LAST_MONTH'] or (len(period_type) == 6 and period_type.isdigit()):
        return 12
    elif 'QUARTER' in period_type or (len(period_type) == 6 and 'Q' in period_type):
        return 4
    return 1

def calculate_coverage(actual, target):
    """Calculate coverage percentage"""
    if target <= 0:
        return 0
    return round((actual / target) * 100, 1)

def calculate_coverage_with_population(doses, population, target_pct, divisor=1):
    """Calculate coverage using population and target percentage"""
    if population <= 0 or target_pct <= 0:
        return 0
    target_pop = (population * target_pct / 100) / divisor
    return round((doses / target_pop) * 100, 1) if target_pop > 0 else 0

def calculate_dropout(first_dose, last_dose):
    """Calculate dropout rate between two doses"""
    if first_dose <= 0:
        return 0
    return round(((first_dose - last_dose) / first_dose) * 100, 1)

def get_color_class(value, target, lower_is_better=False):
    """
    Get color class based on achievement
    Returns: 'green', 'yellow', 'red', or 'gray'
    """
    if value is None:
        return 'gray'
    
    if lower_is_better:
        if value <= target:
            return 'green'
        elif value <= target * 1.5:
            return 'yellow'
        else:
            return 'red'
    else:
        if value >= target:
            return 'green'
        elif value >= target * 0.7:
            return 'yellow'
        else:
            return 'red'

def get_coverage_color(coverage):
    """Get color based on coverage percentage (EPI standard)"""
    if coverage >= 95:
        return "green"
    elif coverage >= 70:
        return "yellow"
    return "red"


# ============ PERIOD HELPERS ============

def generate_monthly_periods(start, end):
    """
    Generate monthly periods between start and end
    Args:
        start: YYYYMM format
        end: YYYYMM format
    Returns: semicolon-separated string of periods
    """
    periods = []
    start_year, start_month = int(start[:4]), int(start[4:6])
    end_year, end_month = int(end[:4]), int(end[4:6])
    y, m = start_year, start_month
    while y < end_year or (y == end_year and m <= end_month):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return ";".join(periods)

def generate_quarters(start_year, start_quarter, end_year, end_quarter):
    """Generate list of quarters between start and end"""
    quarters = []
    year = start_year
    quarter = start_quarter
    
    month_names = {
        1: ('Jan', 'Feb', 'Mar'),
        2: ('Apr', 'May', 'Jun'),
        3: ('Jul', 'Aug', 'Sep'),
        4: ('Oct', 'Nov', 'Dec')
    }
    
    while (year < end_year) or (year == end_year and quarter <= end_quarter):
        quarter_name = f"Q{quarter} {year}"
        months = {
            1: ['01', '02', '03'],
            2: ['04', '05', '06'],
            3: ['07', '08', '09'],
            4: ['10', '11', '12']
        }
        month_codes = [f"{year}{m}" for m in months[quarter]]
        display = f"{month_names[quarter][0]}-{month_names[quarter][2]} {year}"
        
        quarters.append({
            'name': quarter_name,
            'display': display,
            'months': month_codes,
            'period': f"{year}Q{quarter}"
        })
        
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
    
    return quarters

def parse_date_to_quarter(date_str):
    """
    Parse date string to year and quarter
    Args:
        date_str: YYYY-MM format
    Returns: (year, quarter)
    """
    year, month = map(int, date_str.split('-'))
    quarter = (month - 1) // 3 + 1
    return year, quarter


# ============ STATISTICAL HELPERS ============

def detect_outliers_zscore(values, periods=None, threshold=2):
    """
    Detect outliers using Z-score method
    Args:
        values: List of numeric values
        periods: Optional list of period strings (for ICHD detection)
        threshold: Z-score threshold (default 2)
    Returns: List of outlier dictionaries
    """
    if len(values) < 3:
        return []
    
    import statistics
    ICHD_MONTHS = ['04', '10']  # ICHD campaigns in April and October
    
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    
    if std == 0:
        return []
    
    outliers = []
    for i, v in enumerate(values):
        zscore = (v - mean) / std
        is_ichd = False
        
        if periods and i < len(periods):
            month = str(periods[i])[4:6] if len(str(periods[i])) >= 6 else ''
            is_ichd = month in ICHD_MONTHS
        
        # Higher threshold for ICHD months (campaign periods)
        eff_threshold = threshold + 1 if is_ichd else threshold
        
        if abs(zscore) > eff_threshold:
            outliers.append({
                "index": i,
                "value": v,
                "zscore": round(zscore, 2),
                "is_ichd": is_ichd
            })
    
    return outliers

def simple_forecast(values, periods_ahead=3):
    """
    Simple linear regression forecast
    Args:
        values: Historical values
        periods_ahead: Number of periods to forecast
    Returns: List of forecasted values
    """
    if len(values) < 2:
        return []
    
    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    
    slope = numerator / denominator if denominator != 0 else 0
    intercept = y_mean - slope * x_mean
    
    return [round(slope * (n + i) + intercept, 0) for i in range(periods_ahead)]


# ============ DISTRICT NAME HELPERS ============

def clean_district_name(name):
    """
    Remove common suffixes like 'District', 'City', etc. for UBOS lookup
    """
    cleaned = name.upper().strip()
    suffixes = [
        ' DISTRICT', ' CITY', ' MUNICIPALITY', ' TOWN COUNCIL',
        ' SUB COUNTY', ' SUBCOUNTY', ' PARISH',
        ' HC II', ' HC III', ' HC IV', ' HOSPITAL'
    ]
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
    return cleaned

def get_population_for_unit(unit_name, district_name=None):
    """
    Get population for an organizational unit
    Tries multiple methods to find the correct population
    """
    # Try direct lookup
    population = UBOS_POPULATION.get(unit_name.upper(), 0)
    
    if population == 0:
        # Try cleaned version
        population = UBOS_POPULATION.get(clean_district_name(unit_name), 0)
    
    if population == 0 and district_name:
        # Try district name
        population = UBOS_POPULATION.get(district_name.upper(), 0)
        if population == 0:
            population = UBOS_POPULATION.get(clean_district_name(district_name), 0)
    
    return population

