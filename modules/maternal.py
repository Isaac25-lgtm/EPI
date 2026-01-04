"""
Maternal Health Module
======================
ANC (Antenatal Care), Intrapartum, and PNC (Postnatal Care) analytics

This module handles all maternal health indicators including:
- ANC 1st Visit, ANC 1st Trimester, ANC4, ANC8
- IPT3 Coverage, Hb Testing, Iron/Folic Acid supplementation
- Deliveries, LBW, KMC, Birth Asphyxia, Stillbirths
- Postnatal care visits (24hrs, 6 days, 6 weeks)
- Teen pregnancies, Maternal mortality
"""

from flask import Blueprint, jsonify, request, render_template, session
import requests
from requests.auth import HTTPBasicAuth
from functools import wraps
import json
from datetime import datetime

# Create Blueprint
maternal_bp = Blueprint('maternal', __name__, url_prefix='/maternal')

# ============ CONFIGURATION ============

DHIS2_BASE_URL = 'https://hmis.health.go.ug/api'

# UBOS Population Data (copied from main app - should be centralized)
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

# Target percentages for maternal health indicators
# Expected pregnancies = 5% of population
MATERNAL_TARGETS = {
    # ANC Targets
    'anc1_coverage': 100,      # % of expected pregnancies
    'anc1_first_trimester': 45, # % of ANC1 in 1st trimester
    'anc4': 60,                # % completing 4 visits
    'anc8': 20,                # % completing 8 visits
    'ipt3': 85,                # % receiving 3 doses IPT
    'hb_testing': 75,          # % tested for Hb
    'iron_folic': 75,          # % receiving iron/folic acid
    'ultrasound': 40,          # % receiving ultrasound
    'teen_pregnancy': 15,      # % (lower is better)
    
    # Intrapartum Targets
    'deliveries': 68,          # % of expected deliveries in facility
    'lbw': 5,                  # % low birth weight (lower is better)
    'kmc_initiation': 100,     # % of LBW babies initiated on KMC
    'birth_asphyxia': 1,       # % (lower is better)
    'resuscitated': 100,       # % of asphyxiated babies resuscitated
    'fresh_stillbirth': 5,     # per 1000 deliveries (lower is better)
    'neonatal_mortality': 5,   # per 1000 live births (lower is better)
    'perinatal_mortality': 12, # per 1000 (lower is better)
    'maternal_mortality': 20,  # per 100,000 (lower is better)
    
    # PNC Targets
    'breastfeeding_1hr': 90,   # % initiated within 1 hour
    'pnc_24hrs': 90,           # % receiving PNC within 24 hours
    'pnc_6days': 70,           # % receiving PNC at 6 days
    'pnc_6weeks': 70,          # % receiving PNC at 6 weeks
}

# Data Element codes for maternal health (DHIS2)
# These will need to be mapped to actual DHIS2 IDs
MATERNAL_DATA_ELEMENTS = {
    # ANC indicators
    'anc1_visits': '105-AN01a',           # ANC 1st visits
    'anc1_first_trimester': '105-AN01b',  # ANC 1st visit in 1st trimester
    'anc4_visits': '105-AN02',            # ANC 4+ visits
    'anc8_visits': '105-AN03',            # ANC 8+ visits
    'ipt1': '105-AN04a',                  # IPT 1st dose
    'ipt2': '105-AN04b',                  # IPT 2nd dose
    'ipt3': '105-AN04c',                  # IPT 3rd dose
    'hb_tested': '105-AN05',              # Hb tested
    'iron_folic': '105-AN06',             # Iron/Folic acid given
    'ultrasound': '105-AN07',             # Ultrasound done
    'teen_anc1': '105-AN08',              # Teen pregnancies (ANC1 <19 years)
    
    # Delivery/Intrapartum indicators
    'deliveries_facility': '105-DL01',    # Deliveries in facility
    'live_births': '105-DL02',            # Live births
    'lbw_babies': '105-DL03',             # Low birth weight babies
    'kmc_initiated': '105-DL04',          # KMC initiated
    'birth_asphyxia': '105-DL05',         # Birth asphyxia cases
    'resuscitated': '105-DL06',           # Babies resuscitated
    'fresh_stillbirth': '105-DL07',       # Fresh stillbirths
    'macerated_stillbirth': '105-DL08',   # Macerated stillbirths
    'neonatal_death': '105-DL09',         # Neonatal deaths
    'maternal_death': '105-DL10',         # Maternal deaths
    
    # PNC indicators
    'breastfed_1hr': '105-PN01',          # Breastfed within 1 hour
    'pnc_24hrs': '105-PN02',              # PNC within 24 hours
    'pnc_6days': '105-PN03',              # PNC at 6 days
    'pnc_6weeks': '105-PN04',             # PNC at 6 weeks
}


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

def get_expected_pregnancies(population):
    """Calculate expected pregnancies (5% of population)"""
    return round(population * 0.05)

def get_quarterly_target(annual_population, percentage):
    """Calculate quarterly target based on annual population"""
    expected = get_expected_pregnancies(annual_population)
    return round((expected * percentage / 100) / 4)

def calculate_coverage(actual, target):
    """Calculate coverage percentage"""
    if target <= 0:
        return 0
    return round((actual / target) * 100, 1)

def get_color_class(value, target, lower_is_better=False):
    """Get CSS color class based on achievement"""
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

def generate_quarters(start_year, start_quarter, end_year, end_quarter):
    """Generate list of quarters between start and end"""
    quarters = []
    year = start_year
    quarter = start_quarter
    
    while (year < end_year) or (year == end_year and quarter <= end_quarter):
        quarter_name = f"Q{quarter} {year}"
        months = {
            1: ['01', '02', '03'],
            2: ['04', '05', '06'],
            3: ['07', '08', '09'],
            4: ['10', '11', '12']
        }
        month_codes = [f"{year}{m}" for m in months[quarter]]
        
        quarters.append({
            'name': quarter_name,
            'display': f"{'Jan-Mar' if quarter == 1 else 'Apr-Jun' if quarter == 2 else 'Jul-Sep' if quarter == 3 else 'Oct-Dec'} {year}",
            'months': month_codes,
            'period': f"{year}Q{quarter}"
        })
        
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
    
    return quarters


# ============ ROUTES ============

@maternal_bp.route('/')
def maternal_dashboard():
    """Render the maternal health dashboard"""
    if not is_logged_in():
        from flask import redirect, url_for
        return redirect(url_for('login'))
    return render_template('maternal.html')


@maternal_bp.route('/api/data-elements')
@login_required
def get_data_elements():
    """Get maternal health data element IDs from DHIS2"""
    auth = get_auth()
    
    try:
        # Search for maternal health data elements
        patterns = ['105-AN', '105-DL', '105-PN']
        all_elements = []
        
        for pattern in patterns:
            response = requests.get(
                f"{DHIS2_BASE_URL}/dataElements",
                auth=auth,
                params={
                    'filter': f'code:like:{pattern}',
                    'fields': 'id,code,displayName,shortName',
                    'paging': 'false'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                all_elements.extend(data.get('dataElements', []))
        
        return jsonify({
            'dataElements': all_elements,
            'count': len(all_elements)
        })
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@maternal_bp.route('/api/anc-data')
@login_required
def get_anc_data():
    """Get ANC (Antenatal Care) analytics data"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    start_date = request.args.get('startDate', '2023-01')
    end_date = request.args.get('endDate', '2025-03')
    custom_population = request.args.get('customPopulation', None)
    
    # Get population
    if custom_population and custom_population.isdigit():
        population = int(custom_population)
    else:
        population = UBOS_POPULATION.get(district_name, 0)
    
    expected_pregnancies = get_expected_pregnancies(population)
    
    # Generate quarters
    start_y, start_m = map(int, start_date.split('-'))
    end_y, end_m = map(int, end_date.split('-'))
    start_q = (start_m - 1) // 3 + 1
    end_q = (end_m - 1) // 3 + 1
    
    quarters = generate_quarters(start_y, start_q, end_y, end_q)
    
    try:
        # Get data element IDs (in production, these should be cached)
        elements_response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'code:like:105-AN',
                'fields': 'id,code,displayName',
                'paging': 'false'
            },
            timeout=30
        )
        
        if elements_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch data elements'}), 500
        
        elements = elements_response.json().get('dataElements', [])
        code_to_id = {e['code']: e['id'] for e in elements}
        
        # Collect all months for query
        all_months = []
        for q in quarters:
            all_months.extend(q['months'])
        
        pe_dimension = ";".join(all_months)
        dx_ids = list(code_to_id.values())
        
        if not dx_ids:
            return jsonify({
                'error': 'No ANC data elements found',
                'quarters': [],
                'targets': MATERNAL_TARGETS
            })
        
        dx_dimension = ";".join(dx_ids)
        
        # Fetch analytics
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{pe_dimension}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=60
        )
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'}), 500
        
        data = data_response.json()
        
        # Parse results
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        # Aggregate by period and data element
        id_to_code = {v: k for k, v in code_to_id.items()}
        period_data = {}
        
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            pe = row[pe_idx] if len(row) > pe_idx else ''
            try:
                value = int(float(row[val_idx])) if len(row) > val_idx else 0
            except:
                value = 0
            
            if pe not in period_data:
                period_data[pe] = {}
            
            code = id_to_code.get(dx_id, '')
            if code:
                period_data[pe][code] = period_data[pe].get(code, 0) + value
        
        # Calculate quarterly indicators
        quarterly_target = round(expected_pregnancies / 4)
        results = []
        
        for q in quarters:
            # Sum values for this quarter
            anc1 = sum(period_data.get(m, {}).get('105-AN01a', 0) for m in q['months'])
            anc1_tri = sum(period_data.get(m, {}).get('105-AN01b', 0) for m in q['months'])
            anc4 = sum(period_data.get(m, {}).get('105-AN02', 0) for m in q['months'])
            anc8 = sum(period_data.get(m, {}).get('105-AN03', 0) for m in q['months'])
            ipt3 = sum(period_data.get(m, {}).get('105-AN04c', 0) for m in q['months'])
            hb_test = sum(period_data.get(m, {}).get('105-AN05', 0) for m in q['months'])
            iron = sum(period_data.get(m, {}).get('105-AN06', 0) for m in q['months'])
            ultra = sum(period_data.get(m, {}).get('105-AN07', 0) for m in q['months'])
            teen = sum(period_data.get(m, {}).get('105-AN08', 0) for m in q['months'])
            
            # Calculate coverages
            anc1_cov = calculate_coverage(anc1, quarterly_target)
            anc1_tri_cov = calculate_coverage(anc1_tri, anc1) if anc1 > 0 else 0
            anc4_cov = calculate_coverage(anc4, quarterly_target)
            anc8_cov = calculate_coverage(anc8, quarterly_target)
            ipt3_cov = calculate_coverage(ipt3, anc1) if anc1 > 0 else 0
            hb_cov = calculate_coverage(hb_test, anc1) if anc1 > 0 else 0
            iron_cov = calculate_coverage(iron, anc1) if anc1 > 0 else 0
            ultra_cov = calculate_coverage(ultra, anc1) if anc1 > 0 else 0
            teen_pct = calculate_coverage(teen, anc1) if anc1 > 0 else 0
            
            results.append({
                'period': q['display'],
                'quarter': q['name'],
                'anc1_visits': anc1,
                'anc1_cov': anc1_cov,
                'anc1_cov_color': get_color_class(anc1_cov, MATERNAL_TARGETS['anc1_coverage']),
                'anc1_first_tri': anc1_tri_cov,
                'anc1_first_tri_color': get_color_class(anc1_tri_cov, MATERNAL_TARGETS['anc1_first_trimester']),
                'anc4': anc4_cov,
                'anc4_color': get_color_class(anc4_cov, MATERNAL_TARGETS['anc4']),
                'anc8': anc8_cov,
                'anc8_color': get_color_class(anc8_cov, MATERNAL_TARGETS['anc8']),
                'ipt3': ipt3_cov,
                'ipt3_color': get_color_class(ipt3_cov, MATERNAL_TARGETS['ipt3']),
                'hb_testing': hb_cov,
                'hb_testing_color': get_color_class(hb_cov, MATERNAL_TARGETS['hb_testing']),
                'iron_folic': iron_cov,
                'iron_folic_color': get_color_class(iron_cov, MATERNAL_TARGETS['iron_folic']),
                'ultrasound': ultra_cov,
                'ultrasound_color': get_color_class(ultra_cov, MATERNAL_TARGETS['ultrasound']),
                'teen_pregnancy': teen_pct,
                'teen_pregnancy_color': get_color_class(teen_pct, MATERNAL_TARGETS['teen_pregnancy'], lower_is_better=True),
                'target': quarterly_target
            })
        
        return jsonify({
            'quarters': results,
            'population': population,
            'expected_pregnancies': expected_pregnancies,
            'quarterly_target': quarterly_target,
            'targets': {
                'anc1_coverage': MATERNAL_TARGETS['anc1_coverage'],
                'anc1_first_trimester': MATERNAL_TARGETS['anc1_first_trimester'],
                'anc4': MATERNAL_TARGETS['anc4'],
                'anc8': MATERNAL_TARGETS['anc8'],
                'ipt3': MATERNAL_TARGETS['ipt3'],
                'hb_testing': MATERNAL_TARGETS['hb_testing'],
                'iron_folic': MATERNAL_TARGETS['iron_folic'],
                'ultrasound': MATERNAL_TARGETS['ultrasound'],
                'teen_pregnancy': MATERNAL_TARGETS['teen_pregnancy']
            }
        })
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@maternal_bp.route('/api/intrapartum-data')
@login_required
def get_intrapartum_data():
    """Get Intrapartum (Delivery) analytics data"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    start_date = request.args.get('startDate', '2023-01')
    end_date = request.args.get('endDate', '2025-03')
    custom_population = request.args.get('customPopulation', None)
    
    # Get population
    if custom_population and custom_population.isdigit():
        population = int(custom_population)
    else:
        population = UBOS_POPULATION.get(district_name, 0)
    
    expected_deliveries = get_expected_pregnancies(population)
    
    # Generate quarters
    start_y, start_m = map(int, start_date.split('-'))
    end_y, end_m = map(int, end_date.split('-'))
    start_q = (start_m - 1) // 3 + 1
    end_q = (end_m - 1) // 3 + 1
    
    quarters = generate_quarters(start_y, start_q, end_y, end_q)
    
    try:
        # Get data element IDs
        elements_response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'code:like:105-DL',
                'fields': 'id,code,displayName',
                'paging': 'false'
            },
            timeout=30
        )
        
        if elements_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch data elements'}), 500
        
        elements = elements_response.json().get('dataElements', [])
        code_to_id = {e['code']: e['id'] for e in elements}
        
        # Collect all months
        all_months = []
        for q in quarters:
            all_months.extend(q['months'])
        
        pe_dimension = ";".join(all_months)
        dx_ids = list(code_to_id.values())
        
        if not dx_ids:
            return jsonify({
                'error': 'No delivery data elements found',
                'quarters': [],
                'targets': MATERNAL_TARGETS
            })
        
        dx_dimension = ";".join(dx_ids)
        
        # Fetch analytics
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{pe_dimension}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=60
        )
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'}), 500
        
        data = data_response.json()
        
        # Parse results
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        id_to_code = {v: k for k, v in code_to_id.items()}
        period_data = {}
        
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            pe = row[pe_idx] if len(row) > pe_idx else ''
            try:
                value = int(float(row[val_idx])) if len(row) > val_idx else 0
            except:
                value = 0
            
            if pe not in period_data:
                period_data[pe] = {}
            
            code = id_to_code.get(dx_id, '')
            if code:
                period_data[pe][code] = period_data[pe].get(code, 0) + value
        
        # Calculate quarterly indicators
        quarterly_target = round(expected_deliveries / 4)
        results = []
        
        for q in quarters:
            # Sum values
            deliveries = sum(period_data.get(m, {}).get('105-DL01', 0) for m in q['months'])
            live_births = sum(period_data.get(m, {}).get('105-DL02', 0) for m in q['months'])
            lbw = sum(period_data.get(m, {}).get('105-DL03', 0) for m in q['months'])
            kmc = sum(period_data.get(m, {}).get('105-DL04', 0) for m in q['months'])
            asphyxia = sum(period_data.get(m, {}).get('105-DL05', 0) for m in q['months'])
            resuscitated = sum(period_data.get(m, {}).get('105-DL06', 0) for m in q['months'])
            fresh_still = sum(period_data.get(m, {}).get('105-DL07', 0) for m in q['months'])
            neonatal_death = sum(period_data.get(m, {}).get('105-DL09', 0) for m in q['months'])
            maternal_death = sum(period_data.get(m, {}).get('105-DL10', 0) for m in q['months'])
            
            # Calculate rates
            delivery_cov = calculate_coverage(deliveries, quarterly_target)
            lbw_rate = calculate_coverage(lbw, live_births) if live_births > 0 else 0
            kmc_rate = calculate_coverage(kmc, lbw) if lbw > 0 else None
            asphyxia_rate = calculate_coverage(asphyxia, live_births) if live_births > 0 else 0
            resus_rate = calculate_coverage(resuscitated, asphyxia) if asphyxia > 0 else None
            fresh_still_rate = round((fresh_still / deliveries) * 1000, 1) if deliveries > 0 else 0
            neonatal_rate = round((neonatal_death / live_births) * 1000, 1) if live_births > 0 else 0
            perinatal_rate = round(((fresh_still + neonatal_death) / (deliveries + live_births)) * 1000, 1) if (deliveries + live_births) > 0 else 0
            maternal_rate = round((maternal_death / deliveries) * 100000, 1) if deliveries > 0 else 0
            
            results.append({
                'period': q['display'],
                'quarter': q['name'],
                'deliveries': deliveries,
                'deliveries_cov': delivery_cov,
                'deliveries_color': get_color_class(delivery_cov, MATERNAL_TARGETS['deliveries']),
                'lbw': lbw_rate,
                'lbw_color': get_color_class(lbw_rate, MATERNAL_TARGETS['lbw'], lower_is_better=True),
                'kmc_initiation': kmc_rate,
                'kmc_color': get_color_class(kmc_rate, MATERNAL_TARGETS['kmc_initiation']) if kmc_rate is not None else 'gray',
                'birth_asphyxia': asphyxia_rate,
                'asphyxia_color': get_color_class(asphyxia_rate, MATERNAL_TARGETS['birth_asphyxia'], lower_is_better=True),
                'resuscitated': resus_rate,
                'resuscitated_color': get_color_class(resus_rate, MATERNAL_TARGETS['resuscitated']) if resus_rate is not None else 'gray',
                'fresh_stillbirth': fresh_still_rate,
                'fresh_still_color': get_color_class(fresh_still_rate, MATERNAL_TARGETS['fresh_stillbirth'], lower_is_better=True),
                'neonatal_mortality': neonatal_rate,
                'neonatal_color': get_color_class(neonatal_rate, MATERNAL_TARGETS['neonatal_mortality'], lower_is_better=True),
                'perinatal_mortality': perinatal_rate,
                'perinatal_color': get_color_class(perinatal_rate, MATERNAL_TARGETS['perinatal_mortality'], lower_is_better=True),
                'maternal_mortality': maternal_rate,
                'maternal_color': get_color_class(maternal_rate, MATERNAL_TARGETS['maternal_mortality'], lower_is_better=True),
                'target': quarterly_target
            })
        
        return jsonify({
            'quarters': results,
            'population': population,
            'expected_deliveries': expected_deliveries,
            'quarterly_target': quarterly_target,
            'targets': {
                'deliveries': MATERNAL_TARGETS['deliveries'],
                'lbw': MATERNAL_TARGETS['lbw'],
                'kmc_initiation': MATERNAL_TARGETS['kmc_initiation'],
                'birth_asphyxia': MATERNAL_TARGETS['birth_asphyxia'],
                'resuscitated': MATERNAL_TARGETS['resuscitated'],
                'fresh_stillbirth': MATERNAL_TARGETS['fresh_stillbirth'],
                'neonatal_mortality': MATERNAL_TARGETS['neonatal_mortality'],
                'perinatal_mortality': MATERNAL_TARGETS['perinatal_mortality'],
                'maternal_mortality': MATERNAL_TARGETS['maternal_mortality']
            }
        })
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@maternal_bp.route('/api/pnc-data')
@login_required
def get_pnc_data():
    """Get PNC (Postnatal Care) analytics data"""
    auth = get_auth()
    
    org_unit = request.args.get('orgUnit', 'akV6429SUqu')
    district_name = request.args.get('districtName', '').upper()
    start_date = request.args.get('startDate', '2023-01')
    end_date = request.args.get('endDate', '2025-03')
    custom_population = request.args.get('customPopulation', None)
    
    # Get population
    if custom_population and custom_population.isdigit():
        population = int(custom_population)
    else:
        population = UBOS_POPULATION.get(district_name, 0)
    
    expected_deliveries = get_expected_pregnancies(population)
    
    # Generate quarters
    start_y, start_m = map(int, start_date.split('-'))
    end_y, end_m = map(int, end_date.split('-'))
    start_q = (start_m - 1) // 3 + 1
    end_q = (end_m - 1) // 3 + 1
    
    quarters = generate_quarters(start_y, start_q, end_y, end_q)
    
    try:
        # Get data element IDs
        elements_response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'code:like:105-PN',
                'fields': 'id,code,displayName',
                'paging': 'false'
            },
            timeout=30
        )
        
        if elements_response.status_code != 200:
            return jsonify({'error': 'Failed to fetch data elements'}), 500
        
        elements = elements_response.json().get('dataElements', [])
        code_to_id = {e['code']: e['id'] for e in elements}
        
        # Also need delivery data for denominators
        dl_response = requests.get(
            f"{DHIS2_BASE_URL}/dataElements",
            auth=auth,
            params={
                'filter': 'code:like:105-DL',
                'fields': 'id,code,displayName',
                'paging': 'false'
            },
            timeout=30
        )
        
        if dl_response.status_code == 200:
            dl_elements = dl_response.json().get('dataElements', [])
            for e in dl_elements:
                code_to_id[e['code']] = e['id']
        
        # Collect all months
        all_months = []
        for q in quarters:
            all_months.extend(q['months'])
        
        pe_dimension = ";".join(all_months)
        dx_ids = list(code_to_id.values())
        
        if not dx_ids:
            return jsonify({
                'error': 'No PNC data elements found',
                'quarters': [],
                'targets': MATERNAL_TARGETS
            })
        
        dx_dimension = ";".join(dx_ids)
        
        # Fetch analytics
        params = [
            ('dimension', f'dx:{dx_dimension}'),
            ('dimension', f'pe:{pe_dimension}'),
            ('dimension', f'ou:{org_unit}'),
            ('displayProperty', 'NAME'),
            ('skipMeta', 'false')
        ]
        
        data_response = requests.get(
            f"{DHIS2_BASE_URL}/analytics",
            auth=auth,
            params=params,
            timeout=60
        )
        
        if data_response.status_code != 200:
            return jsonify({'error': f'Analytics error: {data_response.status_code}'}), 500
        
        data = data_response.json()
        
        # Parse results
        headers = data.get('headers', [])
        dx_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'dx'), 0)
        pe_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'pe'), 1)
        val_idx = next((i for i, h in enumerate(headers) if h.get('name') == 'value'), -1)
        if val_idx == -1:
            val_idx = len(headers) - 1
        
        id_to_code = {v: k for k, v in code_to_id.items()}
        period_data = {}
        
        for row in data.get('rows', []):
            dx_id = row[dx_idx] if len(row) > dx_idx else ''
            pe = row[pe_idx] if len(row) > pe_idx else ''
            try:
                value = int(float(row[val_idx])) if len(row) > val_idx else 0
            except:
                value = 0
            
            if pe not in period_data:
                period_data[pe] = {}
            
            code = id_to_code.get(dx_id, '')
            if code:
                period_data[pe][code] = period_data[pe].get(code, 0) + value
        
        # Calculate quarterly indicators
        quarterly_target = round(expected_deliveries / 4)
        results = []
        
        for q in quarters:
            # Sum values
            live_births = sum(period_data.get(m, {}).get('105-DL02', 0) for m in q['months'])
            breastfed = sum(period_data.get(m, {}).get('105-PN01', 0) for m in q['months'])
            pnc_24 = sum(period_data.get(m, {}).get('105-PN02', 0) for m in q['months'])
            pnc_6d = sum(period_data.get(m, {}).get('105-PN03', 0) for m in q['months'])
            pnc_6w = sum(period_data.get(m, {}).get('105-PN04', 0) for m in q['months'])
            
            # Use live births as denominator, or quarterly target if no data
            denom = live_births if live_births > 0 else quarterly_target
            
            # Calculate rates
            breastfed_rate = calculate_coverage(breastfed, denom) if denom > 0 else 0
            pnc_24_rate = calculate_coverage(pnc_24, denom) if denom > 0 else 0
            pnc_6d_rate = calculate_coverage(pnc_6d, denom) if denom > 0 else 0
            pnc_6w_rate = calculate_coverage(pnc_6w, denom) if denom > 0 else 0
            
            results.append({
                'period': q['display'],
                'quarter': q['name'],
                'breastfeeding_1hr': breastfed_rate,
                'breastfeeding_color': get_color_class(breastfed_rate, MATERNAL_TARGETS['breastfeeding_1hr']),
                'pnc_24hrs': pnc_24_rate,
                'pnc_24hrs_color': get_color_class(pnc_24_rate, MATERNAL_TARGETS['pnc_24hrs']),
                'pnc_6days': pnc_6d_rate,
                'pnc_6days_color': get_color_class(pnc_6d_rate, MATERNAL_TARGETS['pnc_6days']),
                'pnc_6weeks': pnc_6w_rate,
                'pnc_6weeks_color': get_color_class(pnc_6w_rate, MATERNAL_TARGETS['pnc_6weeks']),
                'live_births': live_births,
                'target': quarterly_target
            })
        
        return jsonify({
            'quarters': results,
            'population': population,
            'expected_deliveries': expected_deliveries,
            'quarterly_target': quarterly_target,
            'targets': {
                'breastfeeding_1hr': MATERNAL_TARGETS['breastfeeding_1hr'],
                'pnc_24hrs': MATERNAL_TARGETS['pnc_24hrs'],
                'pnc_6days': MATERNAL_TARGETS['pnc_6days'],
                'pnc_6weeks': MATERNAL_TARGETS['pnc_6weeks']
            }
        })
        
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@maternal_bp.route('/api/targets')
def get_targets():
    """Get all maternal health targets"""
    return jsonify(MATERNAL_TARGETS)


@maternal_bp.route('/api/districts')
def get_districts():
    """Get UBOS population data for all districts"""
    return jsonify(UBOS_POPULATION)

