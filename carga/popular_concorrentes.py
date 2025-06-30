import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# === Configuração da conexão ===
url = URL.create(
    drivername="postgresql+pg8000",
    username="postgres",
    password="12345678",
    host="localhost",
    port=5432,
    database="modas"
)
engine = create_engine(url, echo=True, future=True)

# Garante que o schema 'dw' existe
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))

# === Função de normalização de colunas ===
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
          .str.normalize('NFKD')
          .str.encode('ascii', errors='ignore')
          .str.decode('ascii')
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True)
          .str.replace(r"[^\w_]", "", regex=True)
    )
    return df

# === 4) Fato: Shein (concorrente) ===
df_shein = pd.read_csv(
    r"C:/Users/muril/Desktop/PI3/shein_dataset_limpa.csv",
    dtype=str
)
df_shein = normalize_cols(df_shein)
df_shein["price_brl"] = (
    df_shein["price_brl"]
      .str.replace(r"R\$", "", regex=True)
      .str.replace(r"\.", "", regex=True)
      .str.replace(r",", ".", regex=True)
      .astype(float)
)
df_shein.to_sql(
    "fato_shein",
    engine,
    schema="dw",
    if_exists="replace",
    index=False
)


# === 5) Fato: Moda Brasileira ===
df_modabras = pd.read_csv(
    r"C:/Users/muril/Desktop/PI3/moda_feminina_brasileira.csv",
    dtype=str
)
df_modabras = normalize_cols(df_modabras)

# Mapeia mês para número
mes_map = {
    "janeiro":"01","fevereiro":"02","marco":"03","abril":"04","maio":"05","junho":"06",
    "julho":"07","agosto":"08","setembro":"09","outubro":"10","novembro":"11","dezembro":"12"
}
df_modabras["mes_num"] = df_modabras["mes"].str.lower().map(mes_map)

# Cria coluna de data
df_modabras["data_pedido"] = pd.to_datetime(
    df_modabras["ano"] + "-" + df_modabras["mes_num"] + "-" + df_modabras["dia"],
    format="%Y-%m-%d",
    errors="coerce"
)

# Limpa valores monetários
def parse_real(s: pd.Series) -> pd.Series:
    return (
        s.str.replace(r"R\$", "", regex=True)
         .str.replace(r"\.", "", regex=True)
         .str.replace(r",", ".", regex=True)
         .astype(float)
    )
df_modabras["valor"] = parse_real(df_modabras["soma_de_preco_real"])
df_modabras["frete"] = parse_real(df_modabras["soma_de_preco_frete_real"])

# Só entregues
df_modabras = df_modabras[df_modabras["situacao_pedido"].str.lower() == "delivered"]

# Agrupa para fato
df_modabras_fact = (
    df_modabras
      .groupby("id_pedido", as_index=False)
      .agg(
          id_cliente  = ("id_cliente",  "first"),
          data_pedido = ("data_pedido", "first"),
          estado      = ("estado_cliente", "first"),
          cidade      = ("cidade_cliente", "first"),
          valor_total = ("valor",        "sum"),
          frete_total = ("frete",        "sum"),
          qtd_itens   = ("id_produto",   "count")
      )
)
df_modabras_fact["ticket_medio"] = (    
    df_modabras_fact["valor_total"] / df_modabras_fact["qtd_itens"]
)

df_modabras_fact.to_sql(
    "fato_pedidos_modabrasileira",
    engine,
    schema="dw",
    if_exists="replace",
    index=False
)
print(f"→ fato_pedidos_modabrasileira: {len(df_modabras_fact)} linhas carregadas")
print(f"→ fato_shein: {len(df_shein)} linhas carregadas")

