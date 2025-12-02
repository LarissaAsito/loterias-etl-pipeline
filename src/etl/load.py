import pandas as pd
from sqlite3 import Connection

class Loader:
    def load_dfs(self, tables: list, conn: Connection) -> None:
        for table in tables:
            df = pd.read_parquet(f'data/silver/{table}.parquet')
            self._load_to_sql(df, table, conn)

    def _load_to_sql(self, df: pd.DataFrame, table_name: str, conn: Connection) -> None:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    