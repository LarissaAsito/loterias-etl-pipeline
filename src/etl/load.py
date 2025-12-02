import pandas as pd

def load_to_sql(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists="replace", index=False)

def load_dfs(tables, conn):
    
    for table in tables:
        df = pd.read_parquet(f'data/silver/{table}.parquet')
        load_to_sql(df, table, conn)