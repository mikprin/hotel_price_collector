FROM python:3.11-slim

WORKDIR /app

# COPY . .

RUN pip install --no-cache-dir uv

COPY requirements.txt .

RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY hotel_price_absorber_src .

CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info"]
