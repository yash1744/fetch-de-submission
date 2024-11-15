from kafka import KafkaProducer
from collections import defaultdict
import threading
import time
from typing import Dict, Any
from enum import Enum
from src.logger import logger

class ErrorType(Enum):
    """Enumeration of possible error types for classification."""
    PARSING_ERROR = "parsing_error"
    VALIDATION_ERROR = "validation_error"
    PROCESSING_ERROR = "processing_error"
    NETWORK_ERROR = "network_error"
    UNKNOWN_ERROR = "unknown_error"

class ErrorHandler:
    """Handles different types of errors and manages error reporting."""
    def __init__(self, producer: KafkaProducer, dlq_topic: str):
        self.producer = producer
        self.dlq_topic = dlq_topic
        self.error_counts = defaultdict(int)
        self.error_threshold = 100  # Max errors before triggering alert
        
        # Start error monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_errors)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def handle_error(self, error_type: ErrorType, message: Dict[str, Any], error_details: str):
        """Handle different types of errors with appropriate strategies."""
        self.error_counts[error_type] += 1
        
        error_record = {
            'original_message': message,
            'error_type': error_type.value,
            'error_details': error_details,
            'timestamp': str(time.time()),
            'retry_count': message.get('retry_count', 0)
        }
        
        # Log the error
        logger.error(f"Error processing message: {error_details}")
        
        # Send to DLQ if retry limit reached or unrecoverable error
        if error_record['retry_count'] >= 3 or error_type in [ErrorType.VALIDATION_ERROR]:
            self._send_to_dlq(error_record)
        else:
            # Increment retry count and retry processing
            error_record['retry_count'] += 1
            self._retry_processing(error_record)
    
    def _send_to_dlq(self, error_record: Dict[str, Any]):
        """Send failed messages to Dead Letter Queue."""
        try:
            self.producer.send(
                self.dlq_topic,
                value=error_record
            ).get(timeout=10)
            logger.info(f"Message sent to DLQ: {error_record['error_type']}")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {str(e)}")
    
    def _retry_processing(self, error_record: Dict[str, Any]):
        """Retry processing the message after a delay."""
        try:
            time.sleep(2 ** error_record['retry_count'])  # Exponential backoff
            self.producer.send(
                'user-login',  # Original topic
                value=error_record['original_message']
            )
        except Exception as e:
            logger.error(f"Failed to retry message: {str(e)}")
            self._send_to_dlq(error_record)
    
    def _monitor_errors(self):
        """Monitor error rates and trigger alerts if necessary."""
        while True:
            for error_type, count in self.error_counts.items():
                if count >= self.error_threshold:
                    self._trigger_alert(error_type, count)
            time.sleep(60)  # Check every minute
    
    def _trigger_alert(self, error_type: ErrorType, count: int):
        """Trigger alert for high error rates."""
        alert_message = f"HIGH ERROR RATE ALERT: {error_type.value} - Count: {count}"
        logger.critical(alert_message)
        # In production, would integrate with alerting system (e.g., PagerDuty)