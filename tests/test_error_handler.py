import pytest
from src.error_handler import ErrorHandler, ErrorType
from unittest.mock import ANY

# Test the handle_error method
def test_handle_error(mock_producer):
    # Create the ErrorHandler instance using the mock producer
    error_handler = ErrorHandler(mock_producer, "user-login-dlq")

    # Call the handle_error method
    error_handler.handle_error(
        ErrorType.VALIDATION_ERROR,
        {'user_id': '1'},
        "Invalid user ID"
    )

    # Verify that the Kafka producer's send method was called with the expected arguments
    mock_producer.send.assert_called_once_with(
        "user-login-dlq",  # Topic
        value={  # The message sent to the Kafka topic
            'original_message': {'user_id': '1'},
            'error_type': 'validation_error',  # The error type should match
            'error_details': 'Invalid user ID',  # The error details
            'timestamp': ANY,  # Matches any timestamp
            'retry_count': 0  # The retry count should be 0 for the first error
        }
    )

# Test the retry_processing method
def test_retry_processing(mock_producer):
    # Create the ErrorHandler instance using the mock producer
    error_handler = ErrorHandler(mock_producer, "user-login-dlq")

    # Create an error record with a retry count
    error_record = {
        'original_message': {'user_id': '1'},
        'retry_count': 1
    }

    # Call the _retry_processing method (even though it's a private method)
    error_handler._retry_processing(error_record)

    # Verify that the Kafka producer's send method was called with the expected arguments
    mock_producer.send.assert_called_once_with(
        'user-login',  # Topic for retry messages
        value={'user_id': '1'}  # The original message
    )
