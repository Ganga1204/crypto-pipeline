# File: crypto-pipeline/Dockerfile

# Start from Python 3.11 slim (small base image)
FROM python:3.11-slim

# Install Java — PySpark needs it
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Create a working directory inside the container
WORKDIR /app

# Copy requirements first (Docker caches this layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY src/ ./src/

# The default command: run the full pipeline
CMD ["python", "src/run_pipeline.py"]
