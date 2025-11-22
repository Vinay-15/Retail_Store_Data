import pandas as pd

def load_dataset():
    df = pd.read_csv("data/online_retail__compass_9_11.csv")
    return df.to_dict(orient="records")
