import pytest
import time
import json
from unittest.mock import Mock, patch,ANY, MagicMock
from src.user_login_processor import UserLoginProcessor
from src.error_handler import ErrorHandler, ErrorType


@pytest.fixture
def user_login_processor():
    mock_producer = Mock()
    mock_consumer = Mock()
    processor = UserLoginProcessor(
        bootstrap_servers='localhost:9092',
        input_topic='input_topic',
        output_topic='output_topic',
        dlq_topic='dlq_topic'
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

def test_process_message_general_error(user_login_processor):
    message = {'user_id': '1'}
    
    with patch('src.message_validator.MessageValidator.validate_message', return_value=(True, None)):
        with patch.object(user_login_processor, '_enrich_message', side_effect=Exception("Processing error")):
            processed_message = user_login_processor.process_message(message)
            
    assert processed_message is None
    user_login_processor.error_handler.handle_error.assert_called_once_with(
        ErrorType.PROCESSING_ERROR,
        message,
        "Processing error"
    )
    assert user_login_processor.metrics['total_errors'] == 1
    

# def test_json_decode_error_handling():
#     # Mock the required arguments for UserLoginProcessor
#     bootstrap_servers = 'localhost:9092'
#     group_id = 'test-group'
#     topic = 'test-topic'
#     error_handler = MagicMock()

#     processor = UserLoginProcessor(bootstrap_servers, group_id, topic, error_handler)

#     # Mock the message that will cause a JSONDecodeError
#     invalid_message = "invalid json"

#     # Patch the _enrich_message method to raise a JSONDecodeError
#     with patch.object(processor, '_enrich_message', side_effect=json.JSONDecodeError("Expecting value", invalid_message, 0)):
#         processor.process_message(invalid_message)

#     # Verify that the error_handler's handle_error method was called with the expected arguments
#     error_handler.handle_error.assert_called_once_with(
#         ErrorType.PARSING_ERROR,
#         invalid_message,
#         "Expecting value: line 1 column 1 (char 0)"
#     )

#     # Verify that the error count was incremented
#     assert processor.metrics['total_errors'] == 1
