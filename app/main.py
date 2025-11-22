# app/main.py

import os
import io
import pandas as pd
import boto3
from fastapi import FastAPI

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# AWS S3 credentials (from .env)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_FILE_NAME = os.getenv("AWS_FILE_NAME")

# Initialize FastAPI app
app = FastAPI(title="Retail Store Data API")

# -----------------------------
# Function to load dataset
# -----------------------------
def load_dataset(n=None):
    """
    Fetch CSV from S3 and return as pandas DataFrame.
    n: number of rows to load (None = full CSV)
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
        # For large CSVs, read in chunks to avoid memory issues
        chunks = pd.read_csv(io.BytesIO(obj['Body'].read()), chunksize=5000)
        df = pd.concat(chunks, ignore_index=True)
    return df

# -----------------------------
# API Endpoints
# -----------------------------

@app.get("/")
def home():
    """
    Health check endpoint
    """
    return {"message": "Retail Store Data API is running!"}

@app.get("/preview")
def preview(n: int = 5):
    """
    Return first n rows of the dataset
    """
    df = load_dataset(n=n)
    return df.to_dict(orient="records")

@app.get("/data")
def get_data():
    """
    Return the full dataset
    """
    df = load_dataset()
    return df.to_dict(orient="records")
