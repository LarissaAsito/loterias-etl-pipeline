from etl.extract import load_raw_json
from etl.transform import transform_df_raw

def main():
    df_raw = load_raw_json("data/bronze/dataset.json")
    dfs_tables = transform_df_raw(df_raw)
    
    print(dfs_tables)

if __name__ == "__main__":
    main()