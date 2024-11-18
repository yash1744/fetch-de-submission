from kafka import KafkaConsumer, KafkaProducer
import logging
from collections import defaultdict
import threading
import time
import json
from src.message_validator import MessageValidator
from src.error_handler import ErrorHandler, ErrorType
from src.ETL import mask_pii,_enrich_message
from typing import Optional, Dict, Any
from src.logger import logger

class UserLoginProcessor:
    def __init__(self, bootstrap_servers, input_topic, output_topic, dlq_topic):
        """Initialize the processor with Kafka configuration."""
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.dlq_topic = dlq_topic
        
        # Initialize components
        self.producer = self._create_producer()
        self.consumer = self._create_consumer()
        self.error_handler = ErrorHandler(self.producer, self.dlq_topic)
        
        # Metrics storage
        self.metrics = {
            'device_type_counts': defaultdict(int),
            'app_version_counts': defaultdict(int),
            'locale_counts': defaultdict(int),
            'total_processed': 0,
            'total_errors': 0
        }
        
        # Start metrics reporter thread
        self.metrics_thread = threading.Thread(target=self._report_metrics)
        self.metrics_thread.daemon = True
        self.metrics_thread.start()
    
    def _create_consumer(self):
        """Create Kafka consumer with retry logic."""
        retries = 3
        for attempt in range(retries):
            try:
                return KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='earliest',
                    group_id='user_login_processor',
                    enable_auto_commit=True,
                    max_poll_interval_ms=300000  # 5 minutes
                )
            except Exception as e:
                if attempt == retries - 1:
                    logger.error(f"Failed to create consumer after {retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Failed to create consumer, attempt {attempt + 1}/{retries}")
                time.sleep(5)

    def _create_producer(self):
        """Create Kafka producer with retry logic."""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
    def _update_metrics(self, message: Dict[str, Any]):
        """Update metrics based on the processed message."""
        # Increment device type count
        device_type = message.get('device_type', 'unknown')
        self.metrics['device_type_counts'][device_type] += 1

        # Increment app version count
        app_version = message.get('app_version', 'unknown')
        self.metrics['app_version_counts'][app_version] += 1

        # Increment locale count using 'locale' field
        locale = message.get('locale', 'unknown')
        self.metrics['locale_counts'][locale] += 1

        # Increment total processed count
        self.metrics['total_processed'] += 1
    
    def _increment_error_count(self):
        """Increment the error count in the metrics."""
        self.metrics['total_errors'] += 1
            
    def _report_metrics(self):
        """Periodically report processing metrics."""
        while True:
            logger.info("*" * 50+"\n")
            logger.info("Current Processing Metrics:")
            logger.info(f"Total messages processed: {self.metrics['total_processed']}")
            logger.info(f"Device type distribution: {dict(self.metrics['device_type_counts'])}")
            logger.info(f"App version distribution: {dict(self.metrics['app_version_counts'])}")
            logger.info(f"locale distribution: {dict(self.metrics['locale_counts'])}")
            logger.info(f"Total errors: {self.metrics['total_errors']}\n")
            logger.info("*" * 50)
            time.sleep(5)  # Report every 20 seconds

    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single message with error handling."""
        try:
            # Validate message

            valid, error_message = MessageValidator.validate_message(message)
            if not valid:
                self.error_handler.handle_error(
                    ErrorType.VALIDATION_ERROR,
                    message,
                    error_message
                )
                self._increment_error_count()
                return None
            else:
                message = mask_pii(message)
                
            # Enrich message
            processed_message = _enrich_message(message)
            logger.info(f"Processed message: {processed_message}")
            
            # Update metrics
            self._update_metrics(processed_message)
            
            return processed_message
            
        except json.JSONDecodeError as e:
            self.error_handler.handle_error(
                ErrorType.PARSING_ERROR,
                message,
                str(e)
            )
            self._increment_error_count()
        except Exception as e:
            self.error_handler.handle_error(
                ErrorType.PROCESSING_ERROR,
                message,
                str(e)
            )
            self._increment_error_count()
        return None

    

    def run(self):
        """Main processing loop with error handling."""
        logger.info(f"Starting processing from topic: {self.input_topic}")
        
        while True:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for _, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process message
                            processed_data = self.process_message(message.value)
                            
                            if processed_data:
                                # Send to output topic
                                self.producer.send(
                                    self.output_topic,
                                    value=processed_data
                                ).get(timeout=10)
                                
                                # Commit offset only after successful processing
                                self.consumer.commit()
                            
                        except Exception as e:
                            self.error_handler.handle_error(
                                ErrorType.PROCESSING_ERROR,
                                message.value,
                                str(e)
                            )
                            self._increment_error_count()
                
            except Exception as e:
                logger.error(f"Fatal error in processing loop: {str(e)}")
                self.error_handler.handle_error(
                    ErrorType.UNKNOWN_ERROR,
                    {},
                    str(e)
                )
                self._increment_error_count()
                time.sleep(5)  # Avoid tight error loop

    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        self.consumer.close()
        self.producer.close()