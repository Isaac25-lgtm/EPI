"""
Authentication Module
Handles login, logout, and session management
"""
from flask import Blueprint, request, jsonify, render_template, session, redirect, url_for
from requests.auth import HTTPBasicAuth
import requests

from .core import DHIS2_BASE_URL, DHIS2_TIMEOUT, is_logged_in, http_session

# Create Blueprint
auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Handle user login"""
    if request.method == 'POST':
        data = request.get_json() if request.is_json else request.form
        username = data.get('username', '')
        password = data.get('password', '')
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'Username and password required'})
        
        # Test credentials against DHIS2
        try:
            response = http_session.get(
                f"{DHIS2_BASE_URL}/me",
                auth=HTTPBasicAuth(username, password),
                params={'fields': 'id,displayName'},
                timeout=15
            )
            if response.status_code == 200:
                user_data = response.json()
                session['username'] = username
                session['password'] = password
                session['display_name'] = user_data.get('displayName', username)
                session['user_id'] = user_data.get('id', '')
                return jsonify({'success': True, 'displayName': session['display_name']})
            else:
                return jsonify({'success': False, 'error': 'Invalid credentials'})
        except requests.exceptions.Timeout:
            return jsonify({'success': False, 'error': 'Connection timeout - try again'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    return render_template('login.html')


@auth_bp.route('/logout')
def logout():
    """Handle user logout"""
    session.clear()
    return redirect(url_for('auth.login'))


@auth_bp.route('/api/check-auth')
def check_auth():
    """Check if user is authenticated"""
    if is_logged_in():
        return jsonify({
            'authenticated': True,
            'displayName': session.get('display_name', 'User'),
            'userId': session.get('user_id', '')
        })
    return jsonify({'authenticated': False})


@auth_bp.route('/api/user-info')
def get_user_info():
    """Get current user information"""
    if not is_logged_in():
        return jsonify({'error': 'Not authenticated'})
    return jsonify({
        'displayName': session.get('display_name', 'User'),
        'userId': session.get('user_id', '')
    })

