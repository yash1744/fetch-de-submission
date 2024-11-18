# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#
RUN pip install pytest


# Copy only the required directories into the container
COPY src/ src/
COPY tests/ tests/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Command to run the Kafka consumer
CMD ["python", "src/kafka_consumer.py"]