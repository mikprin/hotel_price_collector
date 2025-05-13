from celery_app import app
from models import SessionLocal, HotelLink
from scrape import scrape_price

@app.task
def scrape_link(url: str):
    result = scrape_price(url)
    # Save to DB, or send to another service, or log
    print(f"Scraped: {result}")

@app.task
def schedule_daily_scrapes():
    session = SessionLocal()
    links = session.query(HotelLink).all()
    for link in links:
        scrape_link.delay(link.url)
    session.close()
