FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
RUN pip install --no-cache-dir uv

COPY requirements.txt .

RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy application code
COPY streamlit_demo.py .

# Expose Streamlit port
EXPOSE 8501

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["streamlit", "run", "streamlit_demo.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]

# Add metadata labels
LABEL maintainer="mikhail.solovyanov@gmail.com" \
      version="1.0.0" \
      description="Streamlit webapp + task scheduler" \
      org.opencontainers.image.licenses="MIT" \
      Name="hotels-price-absorber-streamlit"