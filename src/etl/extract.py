import pandas as pd
from utils.logger import get_logger

class Extractor:
    def __init__(self):
        self.logger = get_logger()
    
    def load_raw_json(self, path: str) -> pd.DataFrame: 
        df_raw = pd.read_json(path)
        self.logger.debug(f"Arquivo bruto lido tem a seguinte dimensao: {len(df_raw)}")
        return df_raw