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
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]