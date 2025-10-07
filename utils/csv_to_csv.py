# excel importado e manualmente separado em 2 CSV 
# este script remove espaços entre dígitos em colunas de anos (1990..2024)
# Edição 2024 (mais recente)
# https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2024_ims_stock_by_sex_destination_and_origin.xlsx
# utils/fix_grouping_spaces_years.py

from pathlib import Path
import pandas as pd
import re

# === caminhos pedidos ===
in_csv  = Path(r"C:\PythonProjects\emStreamlit\Geografia\data\raw\2024_ims_stock_male_destination_and_origin.csv")
out_csv = Path(r"C:\PythonProjects\emStreamlit\Geografia\data\raw\2024_ims_stock_male_destination_and_origin_sem_sep.csv")

# tentar encodings típicos (Excel PT)
encodings = ["cp1252", "latin-1", "utf-8-sig", "utf-8"]
last_err = None
for enc in encodings:
    try:
        # MUITO IMPORTANTE: separador é ';'
        df = pd.read_csv(in_csv, dtype=str, sep=';', encoding=enc, engine="python")
        used_enc = enc
        break
    except Exception as e:
        last_err = e
else:
    raise last_err

# colunas de anos (1990..2024)
year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c).strip())]

# remover apenas espaços ENTRE dígitos (inclui NBSP/narrow/thin)
space_between_digits = re.compile(r'(?<=\d)[ \u00A0\u202F\u2009](?=\d)')
for c in year_cols:
    df[c] = df[c].astype(str).str.replace(space_between_digits, "", regex=True)

# grava com ; e UTF-8 (Excel abre bem)
df.to_csv(out_csv, index=False, sep=';', encoding="utf-8")
print(f"✅ Gravado: {out_csv}")
print(f"   Encoding de leitura: {used_enc}")
print(f"   Colunas tratadas (anos): {year_cols}")
