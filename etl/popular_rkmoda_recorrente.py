import os
import time
import json
import asyncio
import requests
import httpx
import nest_asyncio
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text

# === CONFIGURAÇÃO POSTGRES ===
DATABASE_URL = "postgresql+psycopg2://admin:1234@localhost:5432/pi3_rkmoda"
engine = create_engine(DATABASE_URL, echo=True, future=True)

# === FUNÇÃO DE NORMALIZAÇÃO DE COLUNAS ===
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

# === CONFIG AWSLI API ===
API_KEY = "377c18d5afee89fd8384"
APP_KEY = "651334f5-8eb8-4d4c-b035-6267d63b7a01"
headers = {
    "Authorization": f"chave_api {API_KEY} aplicacao {APP_KEY}",
    "Content-Type": "application/json"
}

nest_asyncio.apply()

# === COLETA PAGINADA GENÉRICA ===
def coletar_dados(endpoint_base: str, limite_por_pagina: int = 20) -> list:
    dados_totais = []
    offset = 0
    while True:
        params = {"offset": offset, "limit": limite_por_pagina}
        resp = requests.get(endpoint_base, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"Erro {resp.status_code} em {endpoint_base}")
            break
        blob = resp.json()
        objs = blob.get("objects") or blob.get("results") or blob
        if not objs:
            break
        dados_totais.extend(objs)
        print(f"{endpoint_base}: offset {offset} → coletados {len(objs)} (total {len(dados_totais)})")
        if len(objs) < limite_por_pagina:
            break
        offset += limite_por_pagina
    return dados_totais

# === CHAMADAS ASSÍNCRONAS PARA DETALHES DE PEDIDOS ===
async def fetch_pedido(client: httpx.AsyncClient, url: str):
    try:
        r = await client.get(url)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"Erro em {url}: {e}")
    return None

async def fetch_detalhes_bloco(pedidos_bloco: list) -> list:
    detalhes = []
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        tasks = []
        for ped in pedidos_bloco:
            uri = ped.get("resource_uri")
            if uri:
                tasks.append(fetch_pedido(client, f"https://api.awsli.com.br{uri}"))
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            res = await coro
            if res:
                detalhes.append(res)
    return detalhes

# === PARSER DE SKU (cor e tamanho) ===
def extrair_cor_tamanho(sku: str):
    tamanhos = {'pp','p','m','g','gg','xg','un','u','36','38','40','42','44','46'}
    parts = sku.split('-')
    base = parts[0]
    cor = tamanho = None
    for i in range(1, len(parts)):
        p = parts[i].lower()
        if p in tamanhos:
            tamanho = p
            cor = "-".join(parts[1:i]) if i>1 else None
            break
    return base, cor, tamanho

# === ETL PRINCIPAL ===
async def run_etl():
    # 1) pedidos resumo
    resumo = coletar_dados("https://api.awsli.com.br/v1/pedido")
    print(f"Total pedidos resumo: {len(resumo)}")

    # 2) blocos e coleta paralela
    blocos = [resumo[i:i+500] for i in range(0, len(resumo), 500)]
    todos_detalhes = []
    for idx, bloco in enumerate(blocos, start=1):
        print(f"\nProcessando bloco {idx}/{len(blocos)}")
        det = await fetch_detalhes_bloco(bloco)
        todos_detalhes.extend(det)

    # 3) retry para faltantes
    ids_res = {p["numero"] for p in resumo if "numero" in p}
    ids_det = {p["numero"] for p in todos_detalhes if "numero" in p}
    falt = ids_res - ids_det
    retries = 0
    while falt and retries<5:
        print(f"Tentativa {retries+1} para {len(falt)} faltantes")
        pend = [p for p in resumo if p["numero"] in falt]
        det2 = await fetch_detalhes_bloco(pend)
        todos_detalhes.extend(det2)
        ids_det = {p["numero"] for p in todos_detalhes}
        falt = ids_res - ids_det
        retries += 1
        time.sleep(2 + retries*3)
    if falt:
        print(f"Atenção: {len(falt)} pedidos não baixados após retries")

    # 4) normaliza JSON em DataFrames
    df_ped = pd.json_normalize(todos_detalhes, sep="_", max_level=2)
    if 'cliente.resource_uri' in df_ped.columns:
        df_ped['cliente_id'] = df_ped['cliente.resource_uri'].apply(
            lambda uri: uri.strip("/").split("/")[-1] if pd.notna(uri) else None
        )
    pedidos = df_ped.rename(
        columns={'numero':'id_pedido','cliente_id':'id_cliente'}
    )[[ 'data_criacao','id_pedido','valor_desconto',
         'valor_envio','valor_subtotal','valor_total',
         'id_cliente','situacao_nome'
    ]]

    # 5) extrai itens, pagamentos, envios, endereços
    itens = []; pag = []; envs = []; ends = []
    for p in todos_detalhes:
        num = p.get("numero")
        for it in p.get("itens",[]):
            it["numero_pedido"] = num; itens.append(it)
        for pg in p.get("pagamentos",[]):
            pg["numero_pedido"] = num; pag.append(pg)
        for ev in p.get("envios",[]):
            ev["numero_pedido"] = num; envs.append(ev)
        end = p.get("endereco_entrega")
        if end:
            end["numero_pedido"] = num; ends.append(end)

    df_itens = pd.json_normalize(itens, sep="_")
    df_pag = pd.json_normalize(pag, sep="_")
    df_end = pd.json_normalize(ends, sep="_")

    # 6) coleta clientes, produtos e preços
    clientes = coletar_dados("https://api.awsli.com.br/v1/cliente")
    prod     = coletar_dados("https://api.awsli.com.br/v1/produto")
    preco    = coletar_dados("https://api.awsli.com.br/v1/produto_preco")

    # --- MONTAGEM df_cli corrigida ---
    df_cli = pd.json_normalize(clientes, sep="_", max_level=2)[[
        'data_criacao','data_nascimento','email','id','nome','sexo','cpf'
    ]]
    # deduplica endereços por cpf
    cols_end = [c for c in ['cpf','cep','cidade','estado'] if c in df_end.columns]
    df_endereco = df_end[cols_end].drop_duplicates(subset='cpf')
    # merge sem sufixos
    df_cli = df_cli.merge(df_endereco, on='cpf', how='left')
    # renomeia para o DW
    df_cli = df_cli.rename(columns={
        'id':'id_cliente',
        'nome':'nome_cliente',
        'cep':'cep_cliente',
        'cidade':'cidade_cliente',
        'estado':'estado_cliente'
    })[[ 
        'id_cliente','nome_cliente','cpf','sexo',
        'data_nascimento','email','data_criacao',
        'cep_cliente','cidade_cliente','estado_cliente'
    ]]

    # --- MONTAGEM df_produtos ---
    df_prod = pd.json_normalize(prod, sep="_", max_level=2).rename(
        columns={'nome':'nome_produto','id':'id_produto','tipo':'tipo_produto'}
    )[['id_produto','nome_produto','removido','resource_uri','sku','tipo_produto']]
    df_prod['sku'] = df_prod['sku'].fillna('')
    sku_parsed = df_prod['sku'].apply(lambda x: pd.Series(extrair_cor_tamanho(x)))
    sku_parsed.columns = ['todos_sku','cor','tamanho']
    df_prod = pd.concat([df_prod, sku_parsed], axis=1)
    df_preco = pd.json_normalize(preco, sep="_", max_level=2)[
        ['produto','cheio','promocional']
    ].rename(columns={
        'produto':'resource_uri',
        'cheio':'preco_cheio',
        'promocional':'preco_promocional'
    })
    df_prod = df_prod.merge(df_preco, on='resource_uri', how='left')
    df_prod = df_prod.drop_duplicates('sku').reset_index(drop=True)
    df_prod['categoria'] = df_prod['nome_produto'].str.split().str[0]
    df_prod = df_prod.drop(columns=['resource_uri','sku','todos_sku'])

    # --- MONTAGEM fato pedidos_itens ---
    cols_it = ['nome','preco_venda','quantidade','produto','numero_pedido']
    cols_ex = [c for c in cols_it if c in df_itens.columns]
    df_it = df_itens[cols_ex].rename(
        columns={'nome':'nome_produto','produto':'produto_uri','numero_pedido':'id_pedido'}
    )
    df_it['id_produto'] = df_it['produto_uri'].str.extract(r'/produto/(\d+)')
    df_it = df_it.merge(
        pedidos[['id_pedido','valor_envio','valor_subtotal','valor_total','id_cliente','data_criacao','situacao_nome']],
        on='id_pedido', how='left'
    )
    df_it = df_it.merge(
        df_pag[['numero_pedido','forma_pagamento_nome']],
        left_on='id_pedido', right_on='numero_pedido', how='left'
    )
    df_it = df_it.rename(columns={
        'preco_venda':'preco_produto','quantidade':'qtd_produto',
        'valor_envio':'valor_envio_pedido','valor_subtotal':'valor_subtotal_pedido',
        'valor_total':'valor_total_pedido','data_criacao':'data_pedido',
        'situacao_nome':'situacao_pedido','forma_pagamento_nome':'forma_pagamento'
    })
    final_cols = [
        'data_pedido','id_pedido','id_cliente','id_produto','nome_produto',
        'preco_produto','qtd_produto','valor_envio_pedido','valor_subtotal_pedido',
        'valor_total_pedido','situacao_pedido','forma_pagamento'
    ]
    df_it = df_it[[c for c in final_cols if c in df_it.columns]]

    return df_cli, df_prod, df_it

# === MAIN: executa ETL e carrega no Postgres ===
if __name__ == "__main__":
    df_clientes, df_produtos, df_pedidos_itens = asyncio.run(run_etl())

    # normaliza colunas
    df_clientes       = normalize_cols(df_clientes)
    df_produtos       = normalize_cols(df_produtos)
    df_pedidos_itens  = normalize_cols(df_pedidos_itens)

    q = pd.to_numeric(df_pedidos_itens['qtd_produto'].astype(str), errors='coerce')
    mask = q >= 1000
    q.loc[mask] = q.loc[mask] / 1000
    df_pedidos_itens['qtd_produto'] = q.round(0).astype('Int64')

    # garante esquema dw
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))

    # carrega tabelas
    df_clientes.to_sql(
        "dim_cliente", engine, schema="dw",
        if_exists="replace", index=False
    )
    print(f"→ dim_cliente: {len(df_clientes)} linhas")

    df_produtos.to_sql(
        "dim_produto_rk", engine, schema="dw",
        if_exists="replace", index=False
    )
    print(f"→ dim_produto_rk: {len(df_produtos)} linhas")

    df_pedidos_itens.to_sql(
        "fato_pedidos_rk", engine, schema="dw",
        if_exists="replace", index=False
    )
    print(f"→ fato_pedidos_rk: {len(df_pedidos_itens)} linhas")

    print("Carga completa!")
