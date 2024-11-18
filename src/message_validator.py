from typing import Dict, Any, Tuple, Optional
from pydantic import BaseModel, ValidationError, Field, field_validator
import datetime
import ipaddress


class MessageSchema(BaseModel):
    user_id: str = Field(description="Unique identifier for the user")
    app_version: str = Field(description="Version of the application")
    device_type: str = Field(description="Type of the device, e.g., android, ios")
    ip: str = Field(description="IP address of the user")
    locale: str = Field(description="User locale")
    device_id: str = Field(description="Unique identifier for the device")
    timestamp: int = Field(description="Timestamp of the event in ISO 8601 format or as an integer")
    
    @field_validator("ip")
    def validate_ip(cls, v):
        try:
            # Validate using ipaddress module to ensure it's a valid IP (IPv4 or IPv6)
            ipaddress.ip_address(v)
        except ValueError:
            raise ValueError(f"Invalid IP address: {v}")
        return v
    
    
    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        if v <= 0:
            raise ValueError("Timestamp must be positive")
        return v

class MessageValidator:
    """Validates incoming messages against the expected schema."""

    @staticmethod
    def validate_message(message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate message against schema.
        Returns (is_valid, error_message)
        """
        try:
            # Use model_validate for validation
            MessageSchema.model_validate(message)
            return True, None  # Message is valid

        except ValidationError as e:
            # Collect error messages from Pydantic and join into a single string
            error_message = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
            return False, error_message
