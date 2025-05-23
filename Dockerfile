FROM selenium/standalone-chrome:4.32.0-20250515

# Set environment variables to install packages into the system Python environment
ENV UV_PROJECT_ENVIRONMENT=/usr/local \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /home/seluser

RUN sudo chown -R seluser:seluser .local/
RUN mkdir -p ./venv
RUN uv venv --no-cache-dir -p python3.11 ./venv/

# Set the PATH to include the virtual environment's bin directory
ENV PATH="/opt/venv/bin:$PATH"

# Copy dependency files
COPY requirements.txt /home/seluser/

# Install dependencies into the system Python environment
RUN uv pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY hotel_price_absorber_src /home/seluser/hotel_price_absorber_src/

ENV SE_OFFLINE=true

# Add hotel_price_absorber_src to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/home/seluser/"


# Set the default command
CMD ["rq", "worker"]

# Add metadata labels
LABEL maintainer="mikhail.solovyanov@gmail.com" \
      version="1.0.0" \
      description="RQ worker with Selenium support" \
      org.opencontainers.image.source="https://github.com/yourorg/selenium-rq-worker" \
      org.opencontainers.image.licenses="MIT" \
      Name="hotels-price-absorber-worker"
