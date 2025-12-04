from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import Loader
from db.connection import ConnectionDB
from utils.logger import get_logger

class Pipeline:
    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()
        self.connection = ConnectionDB()
        self.logger = get_logger()
    
    def run(self):
        self.logger.info("Starting extraction phase...")
        df_raw = self.extractor.load_raw_json("data/bronze/dataset.json")
        self.logger.info("Starting transformation phase...")
        dfs_tables = self.transformer.transform_df_raw(df_raw)
        self.logger.info("Starting loading phase...")
        conn = self.connection.get_connection('data/gold/loterias.db')
        self.loader.load_dfs(dfs_tables.keys(), conn)