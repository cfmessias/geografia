import pandas as pd

# Carregar sem nenhuma conversão
df_raw = pd.read_csv("data/demografia_mundial.csv", sep=";", encoding="utf-8", 
                     skipinitialspace=True, low_memory=False, dtype=str)

densidade_col = "Population Density, as of 1 July (persons per square km)"

print("=== VALORES ORIGINAIS (primeiros 20 não-nulos) ===")
valores_originais = df_raw[densidade_col].dropna().head(20)
for i, val in enumerate(valores_originais):
    print(f"{i}: '{val}' (tipo: {type(val)})")

print(f"\n=== ESTATÍSTICAS ===")
print(f"Total de linhas: {len(df_raw)}")
print(f"Valores nulos originais: {df_raw[densidade_col].isna().sum()}")
print(f"Valores vazios (''): {(df_raw[densidade_col] == '').sum()}")

# Ver valores únicos (primeiros 50)
valores_unicos = df_raw[densidade_col].dropna().unique()[:50]
print(f"\nPrimeiros 50 valores únicos:")
print(valores_unicos)