# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy application code
COPY . .

# Create directory for local database
RUN mkdir -p /app/data

# Set environment variables
ENV FLASK_ENV=production
ENV PORT=8080

# Expose port
EXPOSE 8080

# Run with Gunicorn (use shell form to expand $PORT)
CMD ["/bin/sh", "-c", "gunicorn wsgi:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --threads 4 --timeout 120"]
