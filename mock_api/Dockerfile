FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port 8000 (uvicorn default in main.py logic is 8000, inside container)
EXPOSE 8000

# Running on 0.0.0.0 is crucial for docker networking
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
