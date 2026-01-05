# Heroku/Render Procfile for Production
# Optimized for 10,000+ concurrent users

# Web process with Gunicorn
web: gunicorn -c gunicorn.conf.py wsgi:app

# Alternative with explicit worker count
# web: gunicorn --workers 4 --threads 4 --timeout 120 --bind 0.0.0.0:$PORT wsgi:app
