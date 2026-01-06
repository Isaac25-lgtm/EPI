"""
WSGI entry point for production deployment
Use with Gunicorn for 10,000+ concurrent users

Run with:
    gunicorn -c gunicorn.conf.py wsgi:app

Or directly:
    gunicorn --workers 4 --threads 2 --bind 0.0.0.0:5000 wsgi:app
"""
import os

# Set production environment
os.environ.setdefault('FLASK_ENV', 'production')

from app import create_app

# Create the application instance for WSGI
app = create_app('production')

if __name__ == '__main__':
    app.run()




