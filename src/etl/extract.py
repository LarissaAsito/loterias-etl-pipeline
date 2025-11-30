import pandas as pd

def load_raw_json(path: str) -> pd.DataFrame: 
    df_raw = pd.read_json(path)
    return df_raw