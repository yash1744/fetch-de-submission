# Fetch DE Assignment

# User Login Event Processing Pipeline

## Overview
This application is designed to process user login events from Kafka, validate and enrich the data, and handle error management through a Dead Letter Queue (DLQ) for unprocessable messages. Using a Python-based Kafka consumer and producer, the system reads messages from an input topic, processes them, and either forwards valid messages to an output topic or routes problematic messages to a DLQ topic.

## Architecture and Design Choices
The pipeline leverages Docker for containerization, enabling a consistent runtime environment across development and production. It uses Kafka to ensure reliable data streaming, where each user login event is read by the consumer, validated, enriched, and then either published to an output topic or routed to the DLQ topic if processing errors occur.

A dedicated error handler manages retries and error reporting, enhancing fault tolerance. The application maintains real-time metrics of processed messages, which are logged periodically, providing insight into the device type, app version, locale distribution, and error count.

The pipeline consists of the following components:
- Kafka broker and Zookeeper for message handling
- Python-based data generator
- Custom consumer for data processing and analytics
- Output topic for processed data

# Setup and Installation

1. **Ensure Docker and Docker Compose are installed**:
   - Follow the installation guides for [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).

2. **Clone this repository**:
   - Clone the repository containing the Docker Compose configuration:
     ```sh
     git clone <repository-url>
     cd <repository-directory>
     ```

3. **Start the Kafka environment**:
   - To start the Kafka environment along with other services (like Zookeeper and the consumer service), run:
     ```sh
     docker compose up -d --build
     ```

   - The `-d` flag will run the services in detached mode (background).
   - The `--build` flag will rebuild the images if anything is changed


4. **Consumer Service Logging**

    - Once the environment is set up, the **consumer service** will start running. To monitor its logs use

        ```sh
        docker compose logs -f consumer-service
        ```
5. **Stop the Services**

    - To stop all the services use  :

        ```sh
        docker compose down
        ```
        
### Note: Producer, Consumer, and Generator Combined for Easy Setup

In this setup, the **producer**, **consumer**, and **generator** services are combined into a single `docker-compose.yml` file for simplicity and ease of setup during development.

However, in a **production environment**, these services are typically separated into distinct deployments and communicate over a network layer. This setup allows for independent scaling of each service based on workload parameters, such as Kafka consumer lag, the number of partitions, or the rate at which the generator produces data. Tools like **Kubernetes** or other orchestration platforms can be used to manage this scaling and ensure optimal performance.

## Running tests with Docker :
   - If you're running the tests within a Docker container, you can execute pytest within the container.
     ```sh
     docker exec -it consumer-service pytest
     ```

   - This will execute the tests inside the running container.

## Running Tests with without docker (pytest) 
###### Prerequisites 
Python > 3.7

To run the tests for the consumer service, follow these steps:

1. **Ensure that pytest is installed**:
   - You can install `pytest` by running:
     ```sh
     pip install pytest
     ```

2. **Run the tests**:
   - Navigate to the directory containing your tests and run:
     ```sh
     pytest
     ```

   - By default, `pytest` will discover and run all tests in files named `test_*.py` or `*_test.py`.

3. **Run with specific options**:
   - To see more detailed output, you can use the `-v` flag (verbose mode):
     ```sh
     pytest -v
     ```

4. **Check test results**:
   - After running the tests, pytest will display the test results, including passed, failed, and skipped tests.



## Data Flow
- **Message Consumption**: A Kafka consumer reads user login events from the input topic.
- **Message Validation and Processing**: Each message is validated by `MessageValidator`,
 PII like device_id and ip are masked and enriched with additional metadata like the processing timestamp and latency. 
- **Error Handling**: If errors occur during message processing or enrichment, the error handler routes the message to the DLQ, ensuring failed messages don’t block the pipeline.
- **Output Publishing**: Valid messages are published to the output topic, `processed-user-login`and invalid messages are published to the output topic, `user-login-dlq`, using a Kafka producer. Offsets are committed only after successful message processing.

## Pipeline Efficiency, Scalability, and Fault Tolerance
- **Efficiency**: The consumer utilizes Kafka's poll mechanism with a short timeout, enabling it to process messages in near real-time. Retrying message delivery with `acks='all'` and `retries=3` helps ensure messages are not lost due to network errors.
- **Scalability**: The pipeline can be scaled by deploying multiple consumer instances, leveraging Kafka’s partitioning to distribute load and handle increased traffic.
- **Fault Tolerance**: In the event of validation or processing errors, messages are directed to a DLQ topic, preserving them for later analysis or reprocessing. The error handler also retries connecting to Kafka if it experiences transient issues.


## Production Readiness
### How to deploy in production:
- Containerization: Use Docker to containerize the app for consistent and scalable deployment. Store the Docker image in a registry like AWS ECR or Docker Hub.
- Orchestration: Deploy the Docker container to Kubernetes (or a managed service like EKS, GKE, or AKS). Use Helm charts to configure deployment, autoscaling, and networking settings.
- CI/CD Pipeline: Implement CI/CD with GitHub Actions (or similar) for automated testing, building, and deployment. Include proper secret management (e.g., AWS Secrets Manager) to securely handle sensitive configurations.

### What other components would you want to add to make this production ready?
- Monitoring and Logging: To ensure system health, use Prometheus to collect and store metrics about resource usage, system performance, and Kafka consumer lag, while Grafana helps visualize these metrics for easy monitoring. For log management, use ELK Stack or Fluentd to centralize and analyze logs from your application and infrastructure, making it easier to troubleshoot and maintain.

- Alerting and Auto-scaling: Set up Prometheus Alerts to notify your team about issues like high Kafka lag or service failures, ensuring proactive management. If using kubernetes for orchestration, Enable Kubernetes Horizontal Pod Autoscaler (HPA) to dynamically scale consumer pods based on the incoming traffic or Kafka lag, improving system performance during peak loads.

- Security: Use SSL/TLS to secure Kafka communication and configure Kubernetes Network Policies to restrict access to specific services and improve security. Implement Role-Based Access Control (RBAC) in Kubernetes to restrict access to sensitive resources, ensuring that only authorized users or services can interact with the critical infrastructure.

### How can this application scale with a growing dataset?

- Partitioning Kafka Topics: Increasing the number of partitions in Kafka allows messages to be distributed across multiple brokers, improving throughput and parallel processing. This enables multiple consumers to handle different partitions of the same topic efficiently.

- Horizontal Scaling: Using Orchestration service like Kubernetes enables you to scale consumer pods by adding more replicas to handle growing traffic. The Horizontal Pod Autoscaler (HPA) automatically adjusts the number of pods based on metrics like Kafka message lag or resource usage.

- Distributed Consumer Load Balancing: Kafka Consumer Groups allow multiple consumers to process messages from different partitions concurrently. This distributed approach ensures efficient message processing and prevents any single consumer from being overloaded.

- Data Retention Policies: Kafka’s log retention settings allow for automatic deletion of older messages based on time or size limits. This helps manage storage effectively while archiving older data for future use in scalable storage solutions.

- Data Sharding: Sharding splits large datasets into smaller, manageable pieces distributed across multiple storage nodes. This approach reduces the load on any single node, improving overall system performance as data scales.

