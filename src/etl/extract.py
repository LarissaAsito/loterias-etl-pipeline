import pandas as pd

class Extractor:
    def load_raw_json(self, path: str) -> pd.DataFrame: 
        df_raw = pd.read_json(path)
        return df_raw