# Health Analytics Modules
# Each health program has its own module/blueprint

from flask import Blueprint

# Module registry - will be populated as modules are imported
MODULES = {}

def register_module(name, blueprint, display_name, icon, color_from, color_to, description):
    """Register a health module for the landing page"""
    MODULES[name] = {
        'blueprint': blueprint,
        'display_name': display_name,
        'icon': icon,
        'color_from': color_from,
        'color_to': color_to,
        'description': description
    }

def get_all_modules():
    """Get all registered modules"""
    return MODULES

