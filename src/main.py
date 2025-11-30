from etl.extract import load_raw_json

def main():
    df_raw = load_raw_json("data/bronze/dataset.json")

if __name__ == "__main__":
    main()