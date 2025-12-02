from etl.extract import load_raw_json
from etl.transform import transform_df_raw
from etl.load import load_dfs
from db.connection import get_connection

def main():
    df_raw = load_raw_json("data/bronze/dataset.json")
    dfs_tables = transform_df_raw(df_raw)
    conn = get_connection('data/gold/loterias.db')
    load_dfs(dfs_tables.keys(), conn)
    print(dfs_tables)

if __name__ == "__main__":
    main()