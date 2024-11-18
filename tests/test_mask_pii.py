import pytest
from src.ETL import mask_pii


def test_mask_ip():
    data = {
        "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551
    }
    masked_data = mask_pii(data)
    assert masked_data["ip"] == "199.172.*.*"

def test_mask_device_id():
    data = {
        "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551
    }
    masked_data = mask_pii(data)
    assert masked_data["device_id"] == "****-**-5928"

def test_no_pii_fields():
    data = {
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551
    }
    masked_data = mask_pii(data)
    # No PII to mask, so the data should be unchanged
    assert masked_data["app_version"] == "2.3.0"
    assert masked_data["device_type"] == "android"
    assert masked_data["ip"] == "199.172.*.*"
    assert masked_data["device_id"] == "****-**-5928"
    assert masked_data["timestamp"] == 1694479551

def test_empty_data():
    data = {}
    masked_data = mask_pii(data)
    # Empty dictionary should return empty dictionary
    assert masked_data == {}


def test_mask_pii_with_mixed_data():
    data = {
        "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551,
        "extra_field": "extra_value"
    }
    masked_data = mask_pii(data)
    assert masked_data["ip"] == "199.172.*.*"
    assert masked_data["device_id"] == "****-**-5928"
    assert masked_data["extra_field"] == "extra_value"  # Extra field should not be masked

