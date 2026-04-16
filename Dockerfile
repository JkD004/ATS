FROM python:3.12-slim

WORKDIR /app

# Install system dependencies required for cryptography, grpcio, and compilation
RUN apt-get update && apt-get install -y \
    gcc \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for Docker layer caching
COPY requirements.txt .

# Install python dependencies natively
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application source code
COPY . .

# Expose Uvicorn default port
EXPOSE 8000

# Entry command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
