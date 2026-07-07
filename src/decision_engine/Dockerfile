# Use a production-ready, lightweight PySpark base image
FROM apache/spark:3.5.1-python3

USER root

# Install system dependencies if required (psycopg2-binary usually avoids building from source)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Spark Maven Jar packages to speed up container launch times
# This matches the configuration specified in your SparkSession builder
RUN mkdir -p /opt/spark/jars && \
    curl -sS https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -sS https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar -o /opt/spark/jars/postgresql-42.7.1.jar

# Copy your processing engine script
COPY dispatch_engine.py .

# Switch back to standard spark user for secure container privileges
USER spark

# Run the script directly using python3
CMD ["python3", "dispatch_engine.py"]