# deve ser feito o download manual do ficheiro xlsx, em 
# scripts/build_migration_inout_from_UNDESA.py
import pandas as pd, numpy as np
from pathlib import Path

def extract_inout_from_table1(xlsx_path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(xlsx_path)
    df = pd.read_excel(xls, sheet_name="Table 1", header=10)

    # garantir tipos
    df["Location code of destination"] = pd.to_numeric(df["Location code of destination"], errors="coerce").astype("Int64")
    df["Location code of origin"]      = pd.to_numeric(df["Location code of origin"], errors="coerce").astype("Int64")

    WORLD = 900
    # colunas 'both sexes' são as que são ints (1990, 1995, …, 2024)
    year_cols = [c for c in df.columns if isinstance(c, (int, np.integer))]

    # imigração: destino=país, origem=Mundo
    imm = (df[df["Location code of origin"]==WORLD]
           .rename(columns={
               "Region, development group, country or area of destination":"country",
               "Location code of destination":"m49"})
           )[["m49","country"]+year_cols] \
          .melt(id_vars=["m49","country"], var_name="year", value_name="immigrants")

    # emigração: destino=Mundo, origem=país
    emi = (df[df["Location code of destination"]==WORLD]
           .rename(columns={
               "Region, development group, country or area of origin":"country",
               "Location code of origin":"m49"})
           )[["m49","country"]+year_cols] \
          .melt(id_vars=["m49","country"], var_name="year", value_name="emigrants")

    out = pd.merge(imm, emi, on=["m49","country","year"], how="outer")
    out["year"] = out["year"].astype(int)
    return out.sort_values(["m49","year"])

if __name__ == "__main__":
    # usa o teu ficheiro
    SRC = "data/migration_inout_source.xlsx"   # mete aqui o caminho do xlsx que fizeste download
    DST = Path("data/migration_inout.csv")
    df = extract_inout_from_table1(SRC)
    DST.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DST, index=False, sep=";", encoding="utf-8")
    print(f"OK: {DST} ({len(df):,} linhas)")
