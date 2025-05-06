FROM python:3.9-slim

WORKDIR /app

# Install Java 17 and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH=/app

# Install Kafka CLI tools
RUN mkdir -p /opt/kafka-cli && \
    curl -s https://dlcdn.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz | tar xz -C /opt/kafka-cli --strip-components=1

ENV PATH="/opt/kafka-cli/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /tmp/newspaper-cache

# Make scripts executable
RUN chmod +x /app/*.py

CMD ["python", "main.py"]