from typing import Optional, Dict, Any
import json
import time

def mask_pii(data: dict) -> dict:
    """
    Masks PII in the given dictionary.
    
    Args:
        data (dict): The input dictionary containing possible PII.
    
    Returns:
        dict: The dictionary with PII masked.
    """
    masked_data = data.copy()
    pii_fields = {
        "user_id": lambda x: f"{x[:4]}****{x[-4:]}",  # Partially mask UUID
        "ip": lambda x: ".".join(x.split(".")[:2]) + ".*.*",  # Mask last two segments of IP
        "device_id": lambda x: "****-**-" + x.split("-")[-1],  # Partially mask device_id
    }
    
    for field, mask_fn in pii_fields.items():
        if field in masked_data and isinstance(masked_data[field], str):
            masked_data[field] = mask_fn(masked_data[field])
    
    return masked_data

def _enrich_message( message: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich message with additional data."""
    enriched = message.copy()
    enriched.update({
        'processed_timestamp': int(time.time()),
        'processing_latency': int(time.time()) - int(message['timestamp']),
    })
    return enriched