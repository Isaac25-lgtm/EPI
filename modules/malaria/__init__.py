"""
Malaria Endemic Channel Module
Implements WHO-recommended percentile method for epidemic detection
"""

from flask import Blueprint

malaria_bp = Blueprint('malaria', __name__, url_prefix='/malaria')

from modules.malaria import routes
