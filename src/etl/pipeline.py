from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import Loader
from db.connection import ConnectionDB

class Pipeline:
    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()
        self.connection = ConnectionDB()
    
    def run(self):
        df_raw = self.extractor.load_raw_json("data/bronze/dataset.json")
        dfs_tables = self.transformer.transform_df_raw(df_raw)
        conn = self.connection.get_connection('data/gold/loterias.db')
        self.loader.load_dfs(dfs_tables.keys(), conn)