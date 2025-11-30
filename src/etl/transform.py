import pandas as pd

def explode_normalize(df: pd.DataFrame, col_list: list, key_cols: list = ["loteria", "concurso"]) -> pd.DataFrame:
    if col_list not in df.columns:
        raise ValueError(f"Coluna {col_list} não existe no DataFrame")

    df_filtered = df[df[col_list].astype(bool)][key_cols + [col_list]]
    df_exploded = df_filtered.explode(col_list)
    df_normalized = pd.json_normalize(df_exploded[col_list])

    df_final = pd.concat(
        [df_exploded[key_cols].reset_index(drop=True),
         df_normalized.reset_index(drop=True)],
        axis=1
    )

    return df_final

def explode_dezenas(df: pd.DataFrame, col: str = "dezenasOrdemSorteio", key_cols: list = ["loteria", "concurso"]) -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Coluna {col} não existe no DataFrame")

    df_exp = df[key_cols + [col]].explode(col)
    df_exp["ordem"] = df_exp.groupby(key_cols).cumcount() + 1
    df_exp["dezena"] = df_exp[col]
    df_exp = df_exp.drop(columns=[col])
    return df_exp

def transform_df_raw(df_raw: pd.DataFrame) -> dict:
    df_raw = df_raw.dropna(how="all")
    df_raw = df_raw[df_raw["concurso"].notna()]
    df_raw = df_raw.drop_duplicates(subset=["loteria", "concurso"], keep="first")

    dfs_tables = {}

    dfs_tables['sorteios'] = df_raw.drop(columns=[
        "dezenasOrdemSorteio",
        "dezenas",
        "premiacoes",
        "estadosPremiados",
        "localGanhadores",
        "trevos"
    ])  

    dfs_tables['premiacoes'] = explode_normalize(df_raw, "premiacoes")
    dfs_tables['local_ganhadores'] = explode_normalize(df_raw, "localGanhadores")
    dfs_tables['estados_premiados'] = explode_normalize(df_raw, "estadosPremiados")
    dfs_tables['dezenas_sorteadas'] = explode_dezenas(df_raw)

    for key, df in dfs_tables.items():
        df.to_parquet(f'data/silver/{key}.parquet', engine='pyarrow', compression='snappy')

    return dfs_tables
    