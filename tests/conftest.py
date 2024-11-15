import pytest
from kafka import KafkaProducer, KafkaConsumer
from unittest.mock import MagicMock

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
        "localhost:29092",
        "user-login",
        "processed-user-login",
        "user-login-dlq"
    )