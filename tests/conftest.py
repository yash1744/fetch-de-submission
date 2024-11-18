import pytest
from kafka import KafkaProducer, KafkaConsumer
from unittest.mock import MagicMock
import os

@pytest.fixture
def mock_producer():
    return MagicMock(KafkaProducer)

@pytest.fixture
def mock_consumer():
    return MagicMock(KafkaConsumer)

@pytest.fixture
def mock_error_handler(mock_producer):
    from src.error_handler import ErrorHandler
    return ErrorHandler(mock_producer, "user-login-dlq")

@pytest.fixture
def mock_user_login_processor(mock_producer, mock_consumer, mock_error_handler):
    from  src.user_login_processor import UserLoginProcessor
    return UserLoginProcessor(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"),
        input_topic=os.getenv("INPUT_TOPIC", "user-login"),
        output_topic=os.getenv("OUTPUT_TOPIC", "processed-user-login"),
        dlq_topic=os.getenv("DLQ_TOPIC", "user-login-dlq")
         
    )