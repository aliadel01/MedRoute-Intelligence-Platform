FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY incident_simulator.py .
COPY accidents.csv .

CMD ["python", "incident_simulator.py"]