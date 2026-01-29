FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Initialize database
RUN python init_db.py

# Run the application - Railway uses $PORT env var
CMD python init_db.py && uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
