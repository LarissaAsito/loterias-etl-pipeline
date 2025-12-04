import pandas as pd
import re
from dateutil import parser
from typing import Any

class Transformer:
    def transform_df_raw(self, df_raw: pd.DataFrame) -> dict:
        df_raw = df_raw.dropna(how="all")
        df_raw = df_raw[df_raw["concurso"].notna()]
        df_raw = df_raw.drop_duplicates(subset=["loteria", "concurso"], keep="first")

        df_raw["data"] = df_raw["data"].apply(self._parse_date)
        df_raw["data"] = pd.to_datetime(df_raw["data"]).dt.strftime("%Y-%m-%d")
        df_raw["dataProximoConcurso"] = df_raw["dataProximoConcurso"].apply(self._parse_date)
        df_raw["dataProximoConcurso"] = pd.to_datetime(df_raw["dataProximoConcurso"]).dt.strftime("%Y-%m-%d")

        df_raw = df_raw.applymap(self._trim_recursive)
        dfs_tables = self._generate_normalized_tables(df_raw)

        for key, df in dfs_tables.items():
            df.to_parquet(f'data/silver/{key}.parquet', engine='pyarrow', compression='snappy', index=False)

        return dfs_tables

    def _trim_recursive(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._clean_string(value)
        elif isinstance(value, list):
            return [self._trim_recursive(v) for v in value]
        elif isinstance(value, dict):
            return {k: self._trim_recursive(v) for k, v in value.items()}
        return value

    def _parse_date(self, x: Any) -> Any:
        if pd.isna(x):
            return None
        try:
            return parser.parse(str(x), dayfirst=True).date()
        except:
            return None
    
    def _clean_string(self, s: str) -> str:
        if not isinstance(s, str):
            return s

        s = re.sub(r"[\x00-\x1F\x7F]", "", s)
        s = s.replace("\u200b", "").replace("\xa0", " ")
        s = re.sub(r"\s+", " ", s)
        s = s.strip()
        return s

    def _generate_normalized_tables(self, df_raw: pd.DataFrame) -> dict:
        dfs_tables = {} 
        dfs_tables['sorteios'] = df_raw.drop(columns=[ 
                "dezenasOrdemSorteio", 
                "dezenas", 
                "premiacoes", 
                "estadosPremiados", 
                "localGanhadores", 
                "trevos"
            ]) 
        dfs_tables['premiacoes'] = self._explode_normalize(df_raw, "premiacoes") 
        dfs_tables['local_ganhadores'] = self._explode_normalize(df_raw, "localGanhadores") 
        dfs_tables['estados_premiados'] = self._explode_normalize(df_raw, "estadosPremiados") 
        dfs_tables['trevos'] = self._explode_ordered_list(df_raw, "trevos", "trevo") 
        dfs_tables['dezenas_sorteadas'] = self._explode_ordered_list(df_raw, "dezenasOrdemSorteio", "dezena")
        return dfs_tables
    
    def _explode_normalize(self, df: pd.DataFrame, col_list: list, key_cols: list = ["loteria", "concurso"]) -> pd.DataFrame:
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
    
    def _explode_ordered_list(self, df: pd.DataFrame, col: str, output_col: str, key_cols: list = ["loteria", "concurso"]) -> pd.DataFrame:
        if col not in df.columns:
            raise ValueError(f"Coluna {col} não existe no DataFrame")

        df_exploded = df[key_cols + [col]].explode(col)
        df_exploded["ordem"] = df_exploded.groupby(key_cols).cumcount() + 1
        df_exploded[output_col] = df_exploded[col]
        df_exploded = df_exploded.drop(columns=[col])
        df_exploded = df_exploded[~df_exploded[output_col].isna()]
        return df_exploded