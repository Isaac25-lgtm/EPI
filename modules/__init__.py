"""
Uganda eHMIS Analytics - Modular Health Program Modules
Each health program has its own module for maintainability
"""

# Module registry - add new modules here
MODULES = {
    'epi': 'EPI - Expanded Programme on Immunization',
    'maternal': 'Maternal Health - ANC, PNC, Deliveries',
    'wash': 'WASH - Water, Sanitation & Hygiene',
    'medicine': 'Medicine Tracking',
    'nutrition': 'Nutrition - Malnutrition, Growth Monitoring',
    'hiv': 'HIV - Testing, ART, Viral Suppression',
    'tb': 'TB - Case Detection, Treatment',
    'rbf': 'RBF Assessment - Results-Based Financing',
    'pmtct': 'PMTCT - Prevention of Mother-to-Child Transmission',
    'community': 'Community Health - VHT Activities',
    'malaria': 'Malaria - Cases, Testing, Treatment'
}

def get_active_modules():
    """Return list of active modules"""
    return MODULES

