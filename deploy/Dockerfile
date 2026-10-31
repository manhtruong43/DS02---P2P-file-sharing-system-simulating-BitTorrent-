FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code (.dockerignore keeps .env, .venv, demo data and docs out of the image)
COPY . .

# PYTHONPATH for "src.*" imports; unbuffered output so `docker logs` shows events immediately
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000 50051 50052 50053

CMD ["python", "src/tracker.py"]
