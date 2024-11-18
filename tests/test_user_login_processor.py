import pytest
import time
import json
from unittest.mock import Mock, patch,ANY, MagicMock
from src.user_login_processor import UserLoginProcessor
from src.error_handler import ErrorHandler, ErrorType
from src.ETL import mask_pii,_enrich_message
import os



@pytest.fixture
def user_login_processor():
    mock_producer = Mock()
    mock_consumer = Mock()
    processor = UserLoginProcessor(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"),
        input_topic=os.getenv("INPUT_TOPIC", "user-login"),
        output_topic=os.getenv("OUTPUT_TOPIC", "processed-user-login"),
        dlq_topic=os.getenv("DLQ_TOPIC", "user-login-dlq")
         
    )
    processor.producer = mock_producer
    processor.consumer = mock_consumer
    processor.error_handler = Mock(spec=ErrorHandler)
    return processor

def test_process_message_valid(user_login_processor):
    message = {
        'user_id': '1',
        'device_type': 'mobile',
        'app_version': '1.0',
        'locale': 'en_US',
        'timestamp': int(time.time())
    }
    
    with patch('src.message_validator.MessageValidator.validate_message', return_value=(True, None)):
        processed_message = user_login_processor.process_message(message)
        
    assert processed_message is not None
    assert 'processed_timestamp' in processed_message
    assert 'processing_latency' in processed_message

def test_process_message_validation_error(user_login_processor):
    message = {'user_id': '1'}
    
    with patch('src.message_validator.MessageValidator.validate_message', return_value=(False, "Invalid message")):
        processed_message = user_login_processor.process_message(message)
        
    assert processed_message is None
    user_login_processor.error_handler.handle_error.assert_called_once_with(
        ErrorType.VALIDATION_ERROR,
        message,
        "Invalid message"
    )
    assert user_login_processor.metrics['total_errors'] == 1

def test_process_message_json_decode_error(user_login_processor):
    message = "invalid_json"
    
    with patch('json.loads', side_effect=json.JSONDecodeError("Expecting value", "doc", 0)):
        processed_message = user_login_processor.process_message(message)
        
    assert processed_message is None
    user_login_processor.error_handler.handle_error.assert_called_once_with(
        ErrorType.PROCESSING_ERROR,
        message,
        ANY
    )
    assert user_login_processor.metrics['total_errors'] == 1
