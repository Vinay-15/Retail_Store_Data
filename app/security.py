from fastapi import Header, HTTPException
import os

# Load the API key from environment variables
API_KEY = os.getenv("API_KEY")

def verify_api_key(x_api_key: str = Header(...)):
    """
    Verifies the X-API-Key header against the environment variable.
    Raises 401 Unauthorized if invalid or missing.
    """
    if not API_KEY:
        raise HTTPException(status_code=500, detail="Server API key not configured")
    
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    
    return True