import os
import io
import pandas as pd
import boto3
from fastapi import FastAPI
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("AWS_BUCKET_NAME")
file_name = os.getenv("AWS_FILE_NAME")

app = FastAPI()

def load_dataset():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

@app.get("/")
def home():
    return {"message": "Retail Dataset API is running!"}

@app.get("/data")
def get_data():
    df = load_dataset()
    return df.to_dict(orient="records")

@app.get("/preview")
def preview(n: int = 5):
    df = load_dataset()
    return df.head(n).to_dict(orient="records")
