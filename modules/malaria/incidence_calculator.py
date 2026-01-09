"""
Malaria Incidence Calculator
Calculates incidence rates per 1,000 population
"""

import pandas as pd
import numpy as np
from modules.malaria.utils import safe_float, safe_int


def calculate_incidence(cases, population, multiplier=1000):
    """
    Calculate malaria incidence rate.
    
    Formula: (Confirmed cases / Population) × 1,000
    
    Args:
        cases (float): Number of confirmed malaria cases
        population (float): Population count
        multiplier (int): Multiplier for rate (default 1000)
    
    Returns:
        float: Incidence per 1,000 population, or None if invalid
    """
    cases = safe_float(cases)
    population = safe_float(population)
    
    if population == 0 or population is None:
        return None
    
    return round((cases / population) * multiplier, 2)


def calculate_quartile_classification(incidence_data):
    """
    Classify districts into quartiles based on incidence.
    
    Args:
        incidence_data (dict): {orgunit_id: incidence_value}
    
    Returns:
        tuple: (color_mapping, thresholds)
            - color_mapping: {orgunit_id: {'quartile': 'Q1-Q4', 'color': hex, 'incidence': value}}
            - thresholds: {'q25': value, 'q50': value, 'q75': value}
    """
    # Filter out None values
    valid_values = [v for v in incidence_data.values() if v is not None]
    
    if len(valid_values) == 0:
        # All missing - return gray for everything
        color_mapping = {
            k: {'quartile': 'NO_DATA', 'color': '#BDBDBD', 'incidence': None, 'label': 'No Data'}
            for k in incidence_data.keys()
        }
        return color_mapping, {}
    
    if len(valid_values) < 4:
        # Too few values - use simple median classification
        median = float(np.median(valid_values))
        color_mapping = {}
        
        for orgunit, incidence in incidence_data.items():
            if incidence is None:
                color_mapping[orgunit] = {
                    'quartile': 'NO_DATA',
                    'color': '#BDBDBD',
                    'incidence': None,
                    'label': 'No Data'
                }
            elif incidence <= median:
                color_mapping[orgunit] = {
                    'quartile': 'LOW',
                    'color': '#4CAF50',
                    'incidence': incidence,
                    'label': 'Below Median'
                }
            else:
                color_mapping[orgunit] = {
                    'quartile': 'HIGH',
                    'color': '#F44336',
                    'incidence': incidence,
                    'label': 'Above Median'
                }
        
        return color_mapping, {'median': median}
    
    # Calculate quartiles
    q25 = float(np.percentile(valid_values, 25))
    q50 = float(np.percentile(valid_values, 50))  # Median
    q75 = float(np.percentile(valid_values, 75))
    
    color_mapping = {}
    
    for orgunit, incidence in incidence_data.items():
        if incidence is None:
            quartile = 'NO_DATA'
            color = '#BDBDBD'
            label = 'No Data'
        elif incidence <= q25:
            quartile = 'Q1'
            color = '#4CAF50'  # Green — Low burden
            label = 'Q1 (Low)'
        elif incidence <= q50:
            quartile = 'Q2'
            color = '#FFEB3B'  # Yellow — Moderate
            label = 'Q2 (Moderate)'
        elif incidence <= q75:
            quartile = 'Q3'
            color = '#FF9800'  # Orange — Elevated
            label = 'Q3 (Elevated)'
        else:
            quartile = 'Q4'
            color = '#F44336'  # Red — High burden
            label = 'Q4 (High)'
        
        color_mapping[orgunit] = {
            'quartile': quartile,
            'color': color,
            'incidence': round(incidence, 2) if incidence is not None else None,
            'label': label
        }
    
    thresholds = {
        'q25': round(q25, 2),
        'q50': round(q50, 2),
        'q75': round(q75, 2)
    }
    
    return color_mapping, thresholds


def calculate_weekly_incidence(cases_data, population_data):
    """
    Calculate weekly incidence for multiple org units.
    
    Args:
        cases_data (list): List of dicts with keys: orgunit, period, value
        population_data (dict): {orgunit_id: population}
    
    Returns:
        pandas.DataFrame: Weekly incidence data
    """
    # Convert to DataFrame
    df = pd.DataFrame(cases_data)
    
    if df.empty:
        return df
    
    # Calculate incidence for each row
    df['population'] = df['orgunit'].map(population_data)
    df['incidence'] = df.apply(
        lambda row: calculate_incidence(row['value'], row['population']),
        axis=1
    )
    
    return df


def rank_orgunits_by_incidence(incidence_data):
    """
    Rank organisation units by incidence (highest to lowest).
    
    Args:
        incidence_data (dict): {orgunit_id: incidence_value}
    
    Returns:
        list: Sorted list of (orgunit_id, incidence, rank) tuples
    """
    # Filter out None values
    valid_data = [(ou, inc) for ou, inc in incidence_data.items() if inc is not None]
    
    # Sort by incidence descending
    sorted_data = sorted(valid_data, key=lambda x: x[1], reverse=True)
    
    # Add rank
    ranked_data = [(ou, inc, rank + 1) for rank, (ou, inc) in enumerate(sorted_data)]
    
    return ranked_data


def handle_missing_weeks(data, expected_weeks, orgunit_id):
    """
    Fill in missing weeks with zero cases.
    
    Args:
        data (list): Existing data for org unit
        expected_weeks (list): List of expected period strings
        orgunit_id (str): Organisation unit ID
    
    Returns:
        list: Data with missing weeks filled
    """
    existing_weeks = {d['period'] for d in data}
    
    for week in expected_weeks:
        if week not in existing_weeks:
            data.append({
                'orgunit': orgunit_id,
                'period': week,
                'value': 0,
                'is_imputed': True
            })
    
    return sorted(data, key=lambda x: x['period'])
