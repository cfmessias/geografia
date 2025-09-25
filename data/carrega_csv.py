import pandas as pd
import pyodbc

# === CONFIGURAÇÕES ===
csv_path = "data/demografia_mundial.csv"
tabela_destino = "demografia_mundial"

# === CONEXÃO AO SQL SERVER (Trusted) ===
connection_string = (
    'DRIVER={SQL Server Native Client 11.0};'
    'SERVER=CFMESSIAS\\CFMESSIAS;'
    'DATABASE=Python;'
    'Trusted_Connection=yes;'
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("✅ Conexão ao SQL Server estabelecida com sucesso.")
except Exception as e:
    print("❌ Erro na conexão ao SQL Server:", e)
    exit()

# === LER CSV COM O SEPARADOR CORRETO ===
try:
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8")  # ou encoding='ansi'
    print(f"✅ CSV lido com sucesso: {df.shape[0]} linhas, {df.shape[1]} colunas.")
except Exception as e:
    print("❌ Erro ao ler o ficheiro CSV:", e)
    exit()

# === GERA SQL DINÂMICO PARA INSERT ===
# colunas = list(df.columns)
# colunas_sql = ", ".join(f"[{col}]" for col in colunas)
# placeholders = ", ".join(["?" for _ in colunas])

## Força tudo para string
df = df.astype(str)
df = df.where(pd.notnull(df), None)  # Substitui NaNs por None (NULL no SQL)

# Cria lista de colunas e valores
columns = df.columns.tolist()
values = [tuple(x) for x in df.values]

# Comando INSERT
placeholders = ', '.join(['?'] * len(columns))
insert_sql = f"INSERT INTO {tabela_destino} ({', '.join(columns)}) VALUES ({placeholders})"

# Executa inserção
cursor.fast_executemany = True
cursor.executemany(insert_sql, values)
conn.commit()
print("✅ Dados inseridos com sucesso.")