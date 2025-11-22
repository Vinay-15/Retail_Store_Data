# app/main.py
import os
import io
import pandas as pd
import boto3
from fastapi import FastAPI, Header, HTTPException
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_FILE_NAME = os.getenv("AWS_FILE_NAME")
API_KEY = os.getenv("API_KEY")

app = FastAPI(title="Retail Store Data API")

def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return True

def load_dataset(n=None):
    """
    Load CSV from S3.
    n: number of rows to read (None = full CSV)
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_FILE_NAME)
    if n:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), nrows=n)
    else:
        # Load in chunks to prevent memory crash
        chunks = pd.read_csv(io.BytesIO(obj['Body'].read()), chunksize=5000)
        df = pd.concat(chunks, ignore_index=True)
    return df

@app.get("/")
def home():
    return {"message": "Retail Store Data API is running!"}

@app.get("/preview")
def preview(n: int = 5, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    df = load_dataset(n=n)
    return df.to_dict(orient="records")

@app.get("/data")
def get_data(x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    df = load_dataset()
    return df.to_dict(orient="records")
