# scripts/debug_countries_files.py
from pathlib import Path
import pandas as pd

ROOT = Path(r"C:\PythonProjects\emStreamlit\Geografia")
DATA = ROOT / "data"

def read_csv(p):
    return pd.read_csv(p, sep=";", dtype=str).apply(lambda s: s.str.strip())

def check(p, need):
    print(f"\n== {p} ==")
    if not p.exists():
        print("❌ não existe"); return None
    print(f"tamanho: {p.stat().st_size} bytes")
    df = read_csv(p)
    print("cols:", list(df.columns))
    print("shape:", df.shape)
    miss = [c for c in need if c not in df.columns]
    if miss:
        print("⚠️ faltam colunas:", miss)
    else:
        print("✔️ colunas ok")
    # sanidade
    for c in [c for c in need if c in df.columns]:
        print(f"· non-empty {c}:", df[c].notna().sum())
    print("amostra:")
    print(df.head(3))
    return df

seed_need = ["iso2","iso3","name_en","name_pt","slug"]
prof_need = ["iso3","name","slug"]  # mínimo que a maioria das versões espera

seed = check(DATA/"countries_seed.csv", seed_need)
prof = check(DATA/"countries_profiles.csv", prof_need)

# vê se algum df “serve” para a lista
def usable(df, need):
    return df is not None and all(c in df.columns for c in need) and df[need[0]].notna().any()

print("\nUSABILIDADE:")
print("profiles utilizável:", usable(prof, prof_need))
print("seed utilizável:", usable(seed, seed_need))

print("\nPATHS ABSOLUTOS que a app *devia* ler:")
print(DATA/"countries_profiles.csv")
print(DATA/"countries_seed.csv")
