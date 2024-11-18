import pytest
from src.message_validator import MessageValidator

def test_valid_message():
    message = {
        'user_id': '424cdd21-063a-43a7-b91b-7ca1a833afae',
        'app_version': '2.3.0',
        'device_type': 'android',
        'ip': '199.172.111.135',
        'locale': 'RU',
        'device_id': '593-47-5928',
        'timestamp': '1694479551'
    }
    
    is_valid, error_message = MessageValidator.validate_message(message)
    assert is_valid
    assert error_message is None

def test_missing_required_field():
    message = {
        'app_version': '2.3.0',
        'device_type': 'android',
        'ip': '199.172.111.135',
        'locale': 'RU',
        'device_id': '593-47-5928',
        'timestamp': '1694479551'
    }
    
    is_valid, error_message = MessageValidator.validate_message(message)
    assert not is_valid

def test_invalid_timestamp():
    message = {
        'user_id': '424cdd21-063a-43a7-b91b-7ca1a833afae',
        'app_version': '2.3.0',
        'device_type': 'android',
        'ip': '199.172.111.135',
        'locale': 'RU',
        'device_id': '593-47-5928',
        'timestamp': '-1'
    }
    
    is_valid, error_message = MessageValidator.validate_message(message)
    assert not is_valid

def test_invalid_ip():
    # Invalid IP address
    message = {
        'user_id': '424cdd21-063a-43a7-b91b-7ca1a833afae',
        'app_version': '2.3.0',
        'device_type': 'android',
        'ip': '999.999.999.999',  # Invalid IP
        'locale': 'RU',
        'device_id': '593-47-5928',
        'timestamp': 1694479551  # Correct timestamp type
    }
    
    is_valid, error_message = MessageValidator.validate_message(message)
    assert not is_valid
    assert 'Invalid IP address' in error_message