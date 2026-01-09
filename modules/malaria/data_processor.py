"""
Data Processor for Malaria Endemic Channel
Handles DHIS2 data transformation and preparation
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import requests
from dotenv import load_dotenv

from modules.malaria.config import MALARIA_DATA_ELEMENT, BASELINE_YEARS, DATA_CONFIG
from modules.malaria.utils import (
    get_epi_week, get_epi_year, validate_baseline_data, 
    interpolate_missing_weeks
)

load_dotenv()


class MalariaDataProcessor:
    """Process and prepare malaria data from DHIS2"""
    
    def __init__(self):
        self.dhis2_url = os.getenv('DHIS2_URL')
        self.dhis2_user = os.getenv('DHIS2_USER')
        self.dhis2_pass = os.getenv('DHIS2_PASS')
        
    def fetch_dhis2_data(self, orgunit_id, start_year, end_year):
        """
        Fetch malaria data from DHIS2 Analytics API
        Args:
            orgunit_id: Organization unit ID
            start_year: Start year for data
            end_year: End year for data
        Returns:
            DataFrame with columns: year, epi_week, confirmed_cases, period, orgunit
        """
        try:
            # Build period dimension (weekly periods for all years)
            periods = []
            for year in range(start_year, end_year + 1):
                for week in range(1, 53):
                    periods.append(f"{year}W{week:02d}")
            
            period_str = ";".join(periods)
            
            # Build analytics API URL
            data_element = MALARIA_DATA_ELEMENT['id']
            url = f"{self.dhis2_url}/api/analytics.json"
            
            params = {
                'dimension': [
                    f'dx:{data_element}',
                    f'pe:{period_str}',
                    f'ou:{orgunit_id}'
                ],
                'displayProperty': 'NAME',
                'skipMeta': 'false'
            }
            
            # Make API request
            response = requests.get(
                url, 
                params=params,
                auth=(self.dhis2_user, self.dhis2_pass),
                timeout=120
            )
            
            if response.status_code != 200:
                raise Exception(f"DHIS2 API error: {response.status_code} - {response.text}")
            
            data = response.json()
            
            # Check if we have data
            if 'rows' not in data or len(data['rows']) == 0:
                return pd.DataFrame(columns=['year', 'epi_week', 'confirmed_cases', 'period', 'orgunit'])
            
            # Parse response
            df = self._parse_analytics_response(data)
            
            return df
            
        except Exception as e:
            print(f"Error fetching DHIS2 data: {e}")
            raise
    
    def _parse_analytics_response(self, response_data):
        """
        Parse DHIS2 analytics response into structured DataFrame
        """
        rows = response_data['rows']
        headers = response_data['headers']
        
        # Create DataFrame from rows
        df = pd.DataFrame(rows, columns=[h['name'] for h in headers])
        
        # Rename columns
        column_mapping = {
            'Data': 'data_element',
            'Period': 'period',
            'Organisation unit': 'orgunit',
            'Value': 'confirmed_cases'
        }
        df = df.rename(columns=column_mapping)
        
        # Convert value to numeric
        df['confirmed_cases'] = pd.to_numeric(df['confirmed_cases'], errors='coerce').fillna(0)
        
        # Parse period (format: 2024W01)
        df['year'] = df['period'].str[:4].astype(int)
        df['epi_week'] = df['period'].str[5:].astype(int)
        
        # Select and order columns
        df = df[['year', 'epi_week', 'confirmed_cases', 'period', 'orgunit']]
        
        return df
    
    def prepare_baseline_data(self, orgunit_id, current_year):
        """
        Prepare baseline data (last 5 years)
        Args:
            orgunit_id: Organization unit ID
            current_year: Current year for monitoring
        Returns:
            DataFrame with baseline data
        """
        start_year = current_year - BASELINE_YEARS
        end_year = current_year - 1
        
        print(f"Fetching baseline data: {start_year} to {end_year}")
        
        df = self.fetch_dhis2_data(orgunit_id, start_year, end_year)
        
        if df.empty:
            raise Exception(f"No baseline data found for orgunit {orgunit_id}")
        
        # Validate data quality
        is_valid, message, coverage_stats = validate_baseline_data(
            df, 
            min_weeks=DATA_CONFIG['min_baseline_weeks']
        )
        
        if not is_valid:
            print(f"Warning: {message}")
            print("Coverage stats:", coverage_stats)
        
        # Optionally interpolate missing weeks per year
        if DATA_CONFIG['interpolate_missing']:
            processed_dfs = []
            for year in df['year'].unique():
                year_df = df[df['year'] == year].copy()
                interpolated = interpolate_missing_weeks(
                    year_df[['epi_week', 'confirmed_cases']], 
                    max_gap=DATA_CONFIG['max_missing_weeks']
                )
                interpolated['year'] = year
                interpolated['orgunit'] = orgunit_id
                processed_dfs.append(interpolated)
            
            df = pd.concat(processed_dfs, ignore_index=True)
        
        return df
    
    def prepare_current_data(self, orgunit_id, current_year):
        """
        Prepare current year data for monitoring
        Args:
            orgunit_id: Organization unit ID
            current_year: Year to monitor
        Returns:
            DataFrame with current year data
        """
        print(f"Fetching current year data: {current_year}")
        
        df = self.fetch_dhis2_data(orgunit_id, current_year, current_year)
        
        if df.empty:
            print(f"Warning: No data found for current year {current_year}")
            return pd.DataFrame(columns=['year', 'epi_week', 'confirmed_cases', 'orgunit'])
        
        # Only keep weeks up to current week
        current_epi_week = get_epi_week(datetime.now())
        df = df[df['epi_week'] <= current_epi_week]
        
        return df
    
    def load_from_csv(self, filepath):
        """
        Load data from CSV file (for testing/offline use)
        Expected columns: year, epi_week, confirmed_cases, orgunit
        """
        df = pd.read_csv(filepath)
        
        required_columns = ['year', 'epi_week', 'confirmed_cases', 'orgunit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure correct data types
        df['year'] = df['year'].astype(int)
        df['epi_week'] = df['epi_week'].astype(int)
        df['confirmed_cases'] = pd.to_numeric(df['confirmed_cases'], errors='coerce').fillna(0)
        
        return df
    
    def save_to_csv(self, df, filepath):
        """
        Save processed data to CSV for caching
        """
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")
    
    def aggregate_facilities_to_district(self, df, facility_ids, district_id):
        """
        Aggregate multiple facilities to district level
        Args:
            df: DataFrame with facility data
            facility_ids: List of facility IDs to aggregate
            district_id: Target district ID
        Returns:
            Aggregated DataFrame at district level
        """
        district_df = df[df['orgunit'].isin(facility_ids)].copy()
        
        aggregated = district_df.groupby(['year', 'epi_week']).agg({
            'confirmed_cases': 'sum'
        }).reset_index()
        
        aggregated['orgunit'] = district_id
        
        return aggregated
