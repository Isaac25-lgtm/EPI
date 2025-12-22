import requests
from requests.auth import HTTPBasicAuth

base_url = "https://hmis.health.go.ug/api"
username = "biostat.pader"
password = "Covax.2025"

print("="*70)
print("TESTING DHIS2 CONNECTION")
print("="*70)

# Test authentication first
try:
    response = requests.get(
        f"{base_url}/me",
        auth=HTTPBasicAuth(username, password),
        timeout=30
    )
    print(f"\nAuth Status: {response.status_code}")
    if response.status_code == 200:
        user = response.json()
        print(f"✓ Logged in as: {user.get('displayName', 'Unknown')}")
        print(f"  Username: {user.get('username', 'Unknown')}")
    else:
        print(f"✗ Authentication failed: {response.text[:200]}")
        exit()
except Exception as e:
    print(f"✗ Connection error: {str(e)}")
    exit()

print("\n" + "="*70)
print("SEARCHING FOR ANC DATA ELEMENTS")
print("="*70)

# Search for ANC-related data elements
search_terms = ["ANC", "antenatal", "1st visit", "first visit"]

for term in search_terms:
    print(f"\nSearching for: '{term}'")
    print("-" * 50)
    
    try:
        response = requests.get(
            f"{base_url}/dataElements",
            auth=HTTPBasicAuth(username, password),
            params={
                'filter': f'displayName:ilike:{term}',
                'fields': 'id,name,code,displayName',
                'pageSize': 10
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            elements = data.get('dataElements', [])
            
            if elements:
                print(f"Found {len(elements)} data elements:")
                for elem in elements:
                    print(f"  • ID: {elem.get('id')}")
                    print(f"    Name: {elem.get('displayName', elem.get('name'))}")
                    print(f"    Code: {elem.get('code', 'N/A')}")
                    print()
            else:
                print("  No results found")
        else:
            print(f"  API Error: {response.status_code}")
            
    except Exception as e:
        print(f"  Error: {str(e)}")

print("\n" + "="*70)
print("SEARCHING FOR INDICATORS")
print("="*70)

try:
    response = requests.get(
        f"{base_url}/indicators",
        auth=HTTPBasicAuth(username, password),
        params={
            'filter': 'displayName:ilike:ANC',
            'fields': 'id,name,code,displayName',
            'pageSize': 15
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        indicators = data.get('indicators', [])
        
        if indicators:
            print(f"\nFound {len(indicators)} ANC indicators:")
            for ind in indicators:
                print(f"  • ID: {ind.get('id')}")
                print(f"    Name: {ind.get('displayName', ind.get('name'))}")
                print(f"    Code: {ind.get('code', 'N/A')}")
                print()
        else:
            print("\nNo ANC indicators found")
    else:
        print(f"API Error: {response.status_code}")
        
except Exception as e:
    print(f"Error: {str(e)}")
