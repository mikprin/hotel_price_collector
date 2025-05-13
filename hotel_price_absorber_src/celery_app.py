from celery import Celery
from celery.schedules import crontab

UPDATE_PERIOD = 86400  # 24 hours in seconds

app = Celery(
    'hotel_scraper',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1',
)
app.conf.beat_schedule = {
    'schedule-daily-scraping': {
        'task': 'hotel_price_absorber_src.tasks.schedule_daily_scrapes',
        'schedule': crontab(hour=19, minute=40,),
    },
}
app.conf.timezone = 'UTC'
