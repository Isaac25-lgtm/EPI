import requests
from requests.auth import HTTPBasicAuth

base_url = "https://hmis.health.go.ug/api"
username = "biostat.pader"
password = "Gulu@2025"

print("="*70)
print("SEARCHING FOR ANC INDICATORS")
print("="*70)

indicators = [
    "105-AN01a",
    "105-AN01b",
    "105-AN02",
    "105-AN03"
]

found_indicators = {}

for indicator_code in indicators:
    print(f"\nSearching for: {indicator_code}")
    print("-" * 70)
    
    try:
        response = requests.get(
            f"{base_url}/dataElements",
            auth=HTTPBasicAuth(username, password),
            params={
                'filter': f'code:eq:{indicator_code}',
                'fields': 'id,name,code,displayName,valueType',
                'paging': 'false'
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            elements = data.get('dataElements', [])
            
            if elements:
                for elem in elements:
                    print(f"✓ FOUND!")
                    print(f"  ID: {elem.get('id')}")
                    print(f"  Name: {elem.get('name')}")
                    print(f"  Code: {elem.get('code')}")
                    
                    found_indicators[indicator_code] = {
                        'id': elem.get('id'),
                        'name': elem.get('name'),
                        'code': elem.get('code')
                    }
            else:
                print(f"✗ Not found with exact code. Trying name search...")
                
                response2 = requests.get(
                    f"{base_url}/dataElements",
                    auth=HTTPBasicAuth(username, password),
                    params={
                        'filter': f'name:like:{indicator_code}',
                        'fields': 'id,name,code',
                        'pageSize': 5
                    },
                    timeout=10
                )
                
                if response2.status_code == 200:
                    data2 = response2.json()
                    elements2 = data2.get('dataElements', [])
                    
                    if elements2:
                        print(f"  Found {len(elements2)} similar indicators:")
                        for elem in elements2:
                            print(f"    • {elem.get('name')} (ID: {elem.get('id')})")
        else:
            print(f"✗ API Error: {response.status_code}")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")

print("\n" + "="*70)
print("SUMMARY - COPY THIS TO app.py")
print("="*70)

if found_indicators:
    print("\nDATA_ELEMENTS = {")
    
    for code, info in found_indicators.items():
        var_name = code.replace('-', '_').replace('.', '')
        print(f"    '{var_name}': '{info['id']}',  # {info['name']}")
    
    print("}")
else:
    print("\nNo indicators found.")
