# Health Analytics Dashboard - Uganda HMIS

A comprehensive health analytics dashboard for Uganda HMIS (DHIS2) that tracks **Child Immunization (EPI)**, **Maternal Health (ANC & Intrapartum)**, and **WASH** indicators with coverage analysis, trend forecasting, and RED categorization.

## 🏥 Modules

### 1. 💉 EPI - Child Immunization
- 28 immunization indicators (105-CL01 to 105-CL28)
- Coverage calculations using UBOS population for 146 districts
- Dropout rates: DPT, Polio, BCG→MR1, Malaria 1→2→3→4
- RED Categorization for quarterly performance monitoring
- Trend analysis with outlier detection (Z-score method)
- Forecasting using linear regression

### 2. 🤰 Maternal Health
**ANC (Antenatal Care):**
- ANC 1, 4, 8+ Visit Coverage
- ANC 1st Trimester Rate
- IPT3 Coverage
- Hb Testing, LLIN Distribution
- Iron/Folic Acid, Ultrasound Scan Rates
- Teenage Pregnancy Rate

**Intrapartum:**
- Deliveries Coverage
- Low Birth Weight Rate & KMC Initiation
- Birth Asphyxia & Resuscitation Rates
- Fresh Still Birth Rate (per 1,000)
- Neonatal Mortality Rate (per 1,000)
- Perinatal Mortality Rate (per 1,000)
- Maternal Mortality Ratio (per 100,000)

### 3. 🚿 WASH
- Water, Sanitation, and Hygiene indicators
- Quarterly data analysis

## 📊 Features

- **Multi-level hierarchy**: National → Region → District → Sub-county → Facility
- **UBOS Population data** for all 146 districts
- **Custom catchment populations** for facility-level analysis
- **Period selection**: Monthly, Quarterly, Annual, Custom ranges
- **Color coding**: 🟢 ≥95%, 🟡 70-94.9%, 🔴 <70%
- **Export options**: PDF, PNG, Excel
- **Show Calculation**: Transparent formula display for each indicator
- **Compare feature**: Side-by-side analysis with Excel export

## 📈 Key Formulas

### Coverage
```
Coverage (%) = (Numerator / Target Population) × 100
```

### Dropout Rate
```
Dropout (%) = ((First Dose - Last Dose) / First Dose) × 100
```

### Neonatal Mortality Rate
```
NMR (per 1,000) = (105-MA12 Total Newborn Deaths / Live Births) × 1,000
```

### Maternal Mortality Ratio
```
MMR (per 100,000) = (Maternal Deaths / Live Births) × 100,000
```

## 🚀 Installation

1. Clone the repository:
```bash
git clone https://github.com/Isaac25-lgtm/EPI.git
cd EPI
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file:
```
SECRET_KEY=your-secret-key
```

4. Run the application:
```bash
python app.py
```

5. Open http://localhost:5000 in your browser

6. Login with your **DHIS2 Uganda HMIS credentials**

## 🛠️ Tech Stack

- **Backend**: Flask, Python
- **Frontend**: HTML, CSS, JavaScript
- **Charts**: Chart.js
- **PDF/Image Export**: jsPDF, html2canvas
- **Data Source**: DHIS2 Uganda HMIS (https://hmis.health.go.ug)

## 📁 Project Structure

```
├── app.py              # Main Flask application
├── modules/
│   ├── auth.py         # Authentication
│   ├── core.py         # Core utilities & caching
│   ├── epi.py          # EPI/Immunization module
│   ├── maternal.py     # Maternal Health module
│   └── wash.py         # WASH module
├── templates/
│   ├── dashboard.html  # EPI Dashboard
│   ├── maternal.html   # Maternal Health Dashboard
│   ├── wash.html       # WASH Dashboard
│   ├── landing.html    # Module selection page
│   └── login.html      # Login page
└── requirements.txt    # Python dependencies
```

## 📋 Data Elements

### EPI (105-CL)
| Code | Vaccine |
|------|---------|
| 105-CL01 | BCG |
| 105-CL02 | Hep B zero dose |
| 105-CL04-07 | Polio 0, 1, 2, 3 |
| 105-CL08-09 | IPV 1, 2 |
| 105-CL10-12 | DPT-HepB+Hib 1, 2, 3 |
| 105-CL13-15 | PCV 1, 2, 3 |
| 105-CL16-18 | Rotavirus 1, 2, 3 |
| 105-CL19-21, 26 | Malaria 1, 2, 3, 4 |
| 105-CL22 | Yellow Fever |
| 105-CL23, 27 | Measles (MR1, MR2) |
| 105-CL24, 28 | Fully immunized |

### Maternal Health (105-AN, 105-MA)
| Code | Indicator |
|------|-----------|
| 105-AN01a | ANC 1st Visit |
| 105-AN02 | ANC 4th Visit |
| 105-AN03 | ANC 8+ Visits |
| 105-MA04 | Total Deliveries |
| 105-MA05a1 | Live Births Total |
| 105-MA12 | Newborn Deaths (0-28 days) |
| 105-MA13 | Maternal Deaths |

## 📄 License

MIT

## 👨‍💻 Author

Isaac - [GitHub](https://github.com/Isaac25-lgtm)
