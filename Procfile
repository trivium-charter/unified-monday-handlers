web: gunicorn --worker-class gevent -w 4 app:app
worker: celery -A tasks.celery_app worker --loglevel=info
