FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY consumer.py .
COPY schema.avsc .
CMD ["python", "consumer.py"]
