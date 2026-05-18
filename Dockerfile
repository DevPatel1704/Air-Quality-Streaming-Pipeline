# Use a stable, lightweight Python 3.11 image
FROM python:3.11-slim

# Set environment variables to optimize Python within Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONUTF8=1

# Set the working directory
WORKDIR /app

# Install system dependencies (required for building python packages if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code and dataset
COPY . .

# Ensure standard execution path
ENV PYTHONPATH=/app
