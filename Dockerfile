# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (optional, in case psycopg2 / Supabase needs them)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Vercel expects the app to bind to 0.0.0.0:8080
EXPOSE 8080

# Run with uvicorn
CMD ["uvicorn", "api.index:app", "--host", "0.0.0.0", "--port", "8080"]
