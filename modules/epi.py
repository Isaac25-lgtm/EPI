"""
EPI (Expanded Programme on Immunization) Module
================================================
Child immunization analytics including coverage, dropout rates, 
RED categorization, and trend analysis.

This module handles all EPI indicators including:
- BCG, OPV, DPT-HepB+Hib, PCV, Rotavirus
- Measles/Rubella (MR1, MR2)
- Malaria vaccines (1-4)
- Yellow Fever, IPV
- Fully immunized children (1yr, 2yr)
"""

from flask import Blueprint, jsonify, request, render_template, session
import requests
from requests.auth import HTTPBasicAuth
from functools import wraps
import json
import hashlib
from datetime import datetime, timedelta
import threading

# Create Blueprint
epi_bp = Blueprint('epi', __name__, url_prefix='/epi')

# ============ CONFIGURATION ============

DHIS2_BASE_URL = 'https://hmis.health.go.ug/api'

# UBOS Population Data (Annual figures for 146 districts)
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

# Target population percentages by vaccine
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

# Mapping from DHIS2 codes to target keys
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

# Dropout rate configurations
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


# ============ CACHING ============
class SimpleCache:
    """Thread-safe in-memory cache with expiration"""
    def __init__(self, default_ttl=300):
        self._cache = {}
        self._lock = threading.Lock()
        self.default_ttl = default_ttl
    
    def _make_key(self, *args, **kwargs):
        key_data = json.dumps({'args': args, 'kwargs': sorted(kwargs.items())}, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key):
        with self._lock:
            if key in self._cache:
                item = self._cache[key]
                if datetime.now() < item['expires']:
                    return item['value']
                else:
                    del self._cache[key]
        return None
    
    def set(self, key, value, ttl=None):
        if ttl is None:
            ttl = self.default_ttl
        with self._lock:
            self._cache[key] = {
                'value': value,
                'expires': datetime.now() + timedelta(seconds=ttl),
                'created': datetime.now()
            }
    
    def clear(self):
        with self._lock:
            self._cache.clear()
    
    def stats(self):
        with self._lock:
            now = datetime.now()
            valid = sum(1 for v in self._cache.values() if now < v['expires'])
            return {'total': len(self._cache), 'valid': valid}

# Cache instances for EPI module
epi_cache = SimpleCache(default_ttl=300)
epi_elements_cache = SimpleCache(default_ttl=3600)


# ============ HELPER FUNCTIONS ============

def get_auth():
    """Get authentication from session"""
    if 'username' in session and 'password' in session:
        return HTTPBasicAuth(session['username'], session['password'])
    return None

def is_logged_in():
    """Check if user is logged in"""
    return 'username' in session and 'password' in session

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated_function

def get_period_divisor(period_type):
    """Get divisor based on period type"""
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
    """Get color based on coverage percentage"""
    if coverage >= 95:
        return "green"
    elif coverage >= 70:
        return "yellow"
    return "red"

def calculate_dropout(first_dose, last_dose):
    """Calculate dropout rate"""
    if first_dose <= 0:
        return 0
    return round(((first_dose - last_dose) / first_dose) * 100, 1)

def generate_monthly_periods(start, end):
    """Generate monthly periods between start and end"""
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

def detect_outliers_zscore(values, periods=None, threshold=2):
    """Detect outliers using Z-score method"""
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
        eff_threshold = threshold + 1 if is_ichd else threshold
        if abs(zscore) > eff_threshold:
            outliers.append({"index": i, "value": v, "zscore": round(zscore, 2), "is_ichd": is_ichd})
    return outliers

def simple_forecast(values, periods_ahead=3):
    """Simple linear regression forecast"""
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


# ============ ROUTES ============

@epi_bp.route('/')
def epi_dashboard():
    """Render the EPI dashboard"""
    if not is_logged_in():
        from flask import redirect, url_for
        return redirect(url_for('login'))
    return render_template('dashboard.html')


@epi_bp.route('/api/districts')
def get_districts():
    """Get UBOS population data"""
    return jsonify(UBOS_POPULATION)


@epi_bp.route('/api/targets')
def get_targets():
    """Get target percentages"""
    return jsonify(TARGET_PERCENTAGES)


@epi_bp.route('/api/dropout-configs')
def get_dropout_configs():
    """Get dropout rate configurations"""
    return jsonify(DROPOUT_CONFIGS)

