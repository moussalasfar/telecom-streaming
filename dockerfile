FROM python:3.9-slim

# Install dependencies
RUN pip install kafka-python

# Add your producer script to the container
COPY kafka-producer.py /app/kafka-producer.py
COPY data/sample_data.jsonl /app/sample_data.jsonl

# Set the working directory
WORKDIR /app

# Run the Kafka producer
CMD ["python", "kafka-producer.py"]
