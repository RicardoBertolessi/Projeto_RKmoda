import pandas as pd
from sqlalchemy import create_engine

def executar_carga():
    usuario = 'postgres'
    senha   = '12345678'
    host    = 'localhost'
    porta   = '5432'
    nome    = 'modas'

    engine  = create_engine(f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{nome}")
    caminho_excel = "dados_loja_limpo.xlsx"

    print(" Lendo planilhas do Excel...")
    df_clientes = pd.read_excel(caminho_excel, sheet_name="Clientes")
    df_produtos = pd.read_excel(caminho_excel, sheet_name="Produtos")
    df_pedidos  = pd.read_excel(caminho_excel, sheet_name="Pedidos")

    print(" Enviando dados para o PostgreSQL...")
    df_clientes.to_sql("clientes", engine, if_exists="replace", index=False)
    df_produtos.to_sql("produtos", engine, if_exists="replace", index=False)
    df_pedidos.to_sql("itens_pedidos", engine, if_exists="replace", index=False)

    print(" Dados enviados ao PostgreSQL com sucesso.")
