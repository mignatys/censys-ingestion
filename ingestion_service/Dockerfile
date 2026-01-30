FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code into a package directory
# This allows imports like 'from ingestion_service.database import ...' to work
COPY . ingestion_service/

EXPOSE 8000

# Set PYTHONPATH to include /app so 'ingestion_service' package is found
ENV PYTHONPATH=/app

CMD ["uvicorn", "ingestion_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
