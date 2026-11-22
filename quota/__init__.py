"""Quota-monitoring package."""
from .discovery import sync_nasdaq100_funds
from .jobs import run_quota_check, start_quota_check_async
from .routes import quota_bp
from .schema import init_quota_db


def register_quota(app, db_path_getter):
    """Register the quota API and make sure its schema exists."""
    app.config['QUOTA_DB_PATH_GETTER'] = db_path_getter
    init_quota_db(db_path_getter())
    app.register_blueprint(quota_bp)


def register_quota_jobs(scheduler, db_path_getter):
    scheduler.add_job(
        lambda: run_quota_check(db_path_getter()),
        'cron',
        hour='8,12,17',
        minute=10,
        id='quota_announcement_check',
    )


__all__ = [
    'init_quota_db',
    'register_quota',
    'register_quota_jobs',
    'run_quota_check',
    'start_quota_check_async',
    'sync_nasdaq100_funds',
]
