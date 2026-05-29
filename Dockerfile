FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline ./pipeline

# Overridden by docker-compose (producer vs consumer).
CMD ["python", "-m", "pipeline.consumer"]
