import os
from celery import Celery

CELERY_BROKER_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

broker_use_ssl_config = {}
if CELERY_BROKER_URL.startswith('rediss://'):
    broker_use_ssl_config = {
        'ssl_cert_reqs': 'required',
    }

celery_app = Celery(
    'monday_tasks', # App name
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    # This line is CRITICAL for the worker to discover tasks
    include=['monday_tasks']
)

if broker_use_ssl_config:
    celery_app.conf.broker_use_ssl = broker_use_ssl_config
    celery_app.conf.redis_backend_use_ssl = broker_use_ssl_config

celery_app.conf.timezone = 'America/Los_Angeles'
celery_app.conf.broker_heartbeat = 30 # Configure heartbeat here for Celery 5.x workers
