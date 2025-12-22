# EPI Dashboard - Child Immunization Analytics

A comprehensive immunization analytics dashboard for Uganda HMIS (DHIS2) that tracks child immunization coverage, dropout rates, trends, and forecasts.

## Features

### 📊 Raw Data Section
- View raw doses from DHIS2
- 28 immunization indicators (105-CL01 to 105-CL28)
- Filter by organization unit (6 levels) and time period
- Trend charts and data tables
- Downloadable as PDF and PNG

### 📈 Analytics Section
- **Coverage calculations** using UBOS population for 146 districts
- **Period divisors**: Monthly ÷12, Quarterly ÷4, Annual ÷1
- **Color coding**: 🟢 ≥95%, 🟡 70-94.9%, 🔴 <70%
- **Dropout rates**: DPT, Polio, BCG→MR1, Malaria 1→2→3→4
- **Trend analysis** with outlier detection (Z-score method)
- **Forecasting** using linear regression
- All tables/charts downloadable as PDF and PNG

## Vaccines Covered

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

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Isaac25-lgtm/EPI.git
cd EPI
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file with DHIS2 credentials:
```
DHIS2_USERNAME=your_username
DHIS2_PASSWORD=your_password
```

4. Run the application:
```bash
python app.py
```

5. Open http://localhost:5000 in your browser

## Coverage Formula

```
Coverage (%) = (Doses Administered / (Target% × UBOS Population / Divisor)) × 100
```

## Dropout Rate Formula

```
Dropout (%) = ((First Dose - Last Dose) / First Dose) × 100
```

## Tech Stack

- **Backend**: Flask, Python
- **Frontend**: HTML, CSS, JavaScript
- **Charts**: Chart.js
- **PDF/Image Export**: jsPDF, html2canvas
- **Data Source**: DHIS2 Uganda HMIS

## License

MIT
