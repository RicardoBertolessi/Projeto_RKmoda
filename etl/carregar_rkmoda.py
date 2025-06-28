import requests
import httpx
import asyncio
import pandas as pd
from tqdm import tqdm
import json
import os
import time
import openpyxl
from sqlalchemy import create_engine
import psycopg2

# 🔑 Suas chaves de API
API_KEY = "377c18d5afee89fd8384"
APP_KEY = "651334f5-8eb8-4d4c-b035-6267d63b7a01"

headers = {
    "Authorization": f"chave_api {API_KEY} aplicacao {APP_KEY}",
    "Content-Type": "application/json"
}

def coletar_dados(endpoint_base, limite_por_pagina=20):
    dados_totais = []
    offset = 0
    while True:
        params = {"offset": offset, "limit": limite_por_pagina}
        response = requests.get(endpoint_base, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Erro na requisição para {endpoint_base}: {response.status_code}")
            break
        dados = response.json()
        objetos = dados.get("objects") or dados.get("results") or dados
        if not objetos:
            break
        dados_totais.extend(objetos)
        print(f"{endpoint_base} - Offset {offset}: {len(objetos)} coletados. Total: {len(dados_totais)}")
        if len(objetos) < limite_por_pagina:
            break
        offset += limite_por_pagina
    return dados_totais

async def fetch_pedido(client, url, retries=3, backoff=1):
    for tentativa in range(retries):
        try:
            r = await client.get(url)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"Erro HTTP {r.status_code} para {url}")
        except Exception as e:
            print(f"Erro em {url} (tentativa {tentativa + 1}): {e}")
        await asyncio.sleep(backoff * (tentativa + 1))
    return None

async def fetch_detalhes_bloco(pedidos_bloco):
    detalhes = []
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        tasks = []
        for pedido in pedidos_bloco:
            uri = pedido.get("resource_uri")
            if uri:
                url = f"https://api.awsli.com.br{uri}"
                tasks.append(fetch_pedido(client, url))
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await asyncio.sleep(0)
            res = await future
            if res:
                detalhes.append(res)
    return detalhes

def extrair_ids_pedidos(pedidos):
    return set(p["numero"] for p in pedidos if "numero" in p)

def extrair_id_cliente(resource_uri):
    if pd.isna(resource_uri):
        return None
    return resource_uri.strip("/").split("/")[-1]

def extrair_cor_tamanho(sku):
    tamanhos_validos = {'pp', 'p', 'm', 'g', 'gg', 'xg', 'un', 'u', '36', '38', '40', '42', '44', '46'}
    partes = sku.split('-')
    base = partes[0]
    cor, tamanho = None, None

    for i in range(1, len(partes)):
        parte = partes[i].lower()
        if parte in tamanhos_validos:
            tamanho = parte
            cor = "-".join(partes[1:i]) if i > 1 else None
            break

    return base, cor, tamanho

async def main():
    pedidos_resumo = coletar_dados("https://api.awsli.com.br/v1/pedido")
    print(f"\nTotal pedidos resumo coletados: {len(pedidos_resumo)}")

    blocos = [pedidos_resumo[i:i+500] for i in range(0, len(pedidos_resumo), 500)]

    todos_detalhes = []
    os.makedirs("pedidos_json", exist_ok=True)

    for i, bloco in enumerate(blocos):
        print(f"\n⬇ Processando bloco {i+1}/{len(blocos)} com {len(bloco)} pedidos...")
        detalhes = await fetch_detalhes_bloco(bloco)
        todos_detalhes.extend(detalhes)
        with open(f"pedidos_json/pedidos_bloco_{i+1:02}.json", "w") as f:
            json.dump(detalhes, f)
        print(f" Bloco {i+1} salvo com {len(detalhes)} pedidos.")

    MAX_RETRIES = 5
    ids_resumo = extrair_ids_pedidos(pedidos_resumo)
    ids_detalhes = extrair_ids_pedidos(todos_detalhes)
    faltantes = ids_resumo - ids_detalhes
    print(f"\nPedidos faltantes inicialmente: {len(faltantes)}")

    tentativa = 0
    while faltantes and tentativa < MAX_RETRIES:
        print(f"\n🔁 Tentativa {tentativa+1} para baixar {len(faltantes)} pedidos faltantes...")
        pedidos_para_tentar = [p for p in pedidos_resumo if p["numero"] in faltantes]
        detalhes_faltantes = await fetch_detalhes_bloco(pedidos_para_tentar)
        todos_detalhes.extend(detalhes_faltantes)
        ids_detalhes = extrair_ids_pedidos(todos_detalhes)
        faltantes = ids_resumo - ids_detalhes
        print(f"Pedidos faltantes após tentativa {tentativa+1}: {len(faltantes)}")
        tentativa += 1
        if faltantes:
            espera = 5 + tentativa * 3
            print(f"Aguardando {espera} segundos antes da próxima tentativa...")
            time.sleep(espera)

    if faltantes:
        print(f"{len(faltantes)} pedidos ainda não foram baixados após {MAX_RETRIES} tentativas.")
    else:
        print("Todos os pedidos foram baixados com sucesso!")


    pedidos_df_raw = pd.json_normalize(todos_detalhes, sep="_", max_level=2)

    if 'cliente.resource_uri' in pedidos_df_raw.columns:
        pedidos_df_raw['cliente_id'] = pedidos_df_raw['cliente'].apply(extrair_id_cliente)
    else:
        print("Coluna 'cliente.resource_uri' não encontrada no DataFrame de pedidos.")

    pedidos = pedidos_df_raw[[
        'data_criacao', 'numero', 'valor_desconto', 'valor_envio',
        'valor_subtotal', 'valor_total', 'cliente_id', 'situacao_nome'
    ]].rename(columns={
        'numero': 'id_pedido',
        'cliente_id': 'id_cliente'
    })

    itens, pagamentos, envios, enderecos = [], [], [], []
    for pedido in todos_detalhes:
        numero = pedido.get("numero")
        for item in pedido.get("itens", []):
            item["numero_pedido"] = numero
            itens.append(item)
        for pagamento in pedido.get("pagamentos", []):
            pagamento["numero_pedido"] = numero
            pagamentos.append(pagamento)
        for envio in pedido.get("envios", []):
            envio["numero_pedido"] = numero
            envios.append(envio)
        endereco = pedido.get("endereco_entrega")
        if endereco:
            endereco["numero_pedido"] = numero
            enderecos.append(endereco)

    df_itens = pd.json_normalize(itens, sep="_")
    df_pagamentos = pd.json_normalize(pagamentos, sep="_")
    df_envios = pd.json_normalize(envios, sep="_")
    df_enderecos = pd.json_normalize(enderecos, sep="_")

    colunas_esperadas_itens = [
        'nome', 'preco_cheio', 'preco_promocional', 'preco_subtotal', 'preco_venda',
        'produto', 'produto_pai', 'quantidade', 'numero_pedido'
    ]
    colunas_existentes_itens = [col for col in colunas_esperadas_itens if col in df_itens.columns]

    df_itens_selecionado = df_itens[colunas_existentes_itens].rename(columns={
        'nome': 'nome_produto',
        'produto': 'produto_filho',
        'numero_pedido': 'id_pedido'
    })

    clientes = coletar_dados("https://api.awsli.com.br/v1/cliente")
    produtos = coletar_dados("https://api.awsli.com.br/v1/produto")
    produtos_preco = coletar_dados("https://api.awsli.com.br/v1/produto_preco")


    df_clientes = pd.json_normalize(clientes, sep="_", max_level=2)[[
        'data_criacao', 'data_nascimento', 'email', 'id', 'nome', 'sexo', 'cpf'
    ]]
    df_enderecos = df_enderecos.drop_duplicates(subset='cpf')

    df_clientes = df_clientes.merge(df_enderecos, on='cpf', how='left', suffixes=('_cliente', '_endereco'))
    df_clientes = df_clientes.drop_duplicates()

    df_clientes = df_clientes.rename(columns={
        'id': 'id_cliente',
        'nome': 'nome_cliente',
        'cep': 'cep_cliente',
        'cidade': 'cidade_cliente',
        'estado': 'estado_cliente'
    })

    df_clientes = df_clientes[[
        'id_cliente', 'nome_cliente', 'cpf', 'sexo',
        'data_nascimento', 'email', 'data_criacao',
        'cep_cliente', 'cidade_cliente', 'estado_cliente'
    ]]

    produtos_df = pd.json_normalize(produtos, sep="_", max_level=2)
    produtos_df = produtos_df.rename(columns={
        'nome': 'nome_produto',
        'id': 'id_produto',
        'tipo': 'tipo_produto'
    })

    colunas_uteis = ['id_produto', 'nome_produto', 'removido', 'resource_uri', 'sku', 'tipo_produto']
    produtos_df = produtos_df[colunas_uteis]
    produtos_df['sku'] = produtos_df['sku'].fillna("")

    df_skus = produtos_df['sku'].apply(lambda x: pd.Series(extrair_cor_tamanho(x)))
    df_skus.columns = ['todos_sku', 'cor_estampa', 'numero_tamanho']
    produtos_df = pd.concat([produtos_df, df_skus], axis=1)

    produtos_base = produtos_df[
        produtos_df['cor_estampa'].isna() & produtos_df['numero_tamanho'].isna()
    ][['todos_sku', 'nome_produto']].drop_duplicates()

    produtos_df = produtos_df.merge(produtos_base, on='todos_sku', how='left', suffixes=('', '_base'))
    produtos_df['nome_produto'] = produtos_df['nome_produto'].fillna(produtos_df['nome_produto_base'])
    produtos_df.drop(columns=['nome_produto_base'], inplace=True)

    df_produtos_preco = pd.json_normalize(produtos_preco, sep="_", max_level=2)[
        ['produto', 'cheio', 'promocional']
    ].rename(columns={
        'produto': 'resource_uri',
        'cheio': 'preco_cheio',
        'promocional': 'preco_promocional'
    })

    produtos_df = produtos_df.merge(df_produtos_preco, on='resource_uri', how='left')

    precos_por_nome = produtos_df.groupby('nome_produto')[['preco_cheio', 'preco_promocional']].transform('first')
    produtos_df['preco_cheio'] = produtos_df['preco_cheio'].fillna(precos_por_nome['preco_cheio'])
    produtos_df['preco_promocional'] = produtos_df['preco_promocional'].fillna(precos_por_nome['preco_promocional'])

    produtos_df = produtos_df.drop_duplicates(subset='sku', keep='first').reset_index(drop=True)

    produtos_df = produtos_df[
        (produtos_df['tipo_produto'] != 'normal') &
        (~produtos_df['sku'].str.contains('DUPLICADO', case=False, na=False)) &
        (~produtos_df['nome_produto'].str.contains('DUPLICADO', case=False, na=False)) &
        (~produtos_df['sku'].str.contains('produto-rascunho', case=False, na=False))
    ]

    produtos_df = produtos_df.drop(columns=['resource_uri', 'sku', 'todos_sku'])
    produtos_df['categoria'] = produtos_df['nome_produto'].str.split().str[0]

    df_itens_com_pedidos = df_itens_selecionado.merge(
        pedidos[['id_pedido', 'valor_envio', 'valor_subtotal', 'valor_total', 'id_cliente', 'data_criacao', 'situacao_nome']],
        on='id_pedido',
        how='left'
    )

    df_itens_com_pedidos['id_produto'] = df_itens_com_pedidos['produto_filho'].str.extract(r'/produto/(\d+)')

    pagamentos_para_merge = df_pagamentos[['numero_pedido','forma_pagamento_nome']]

    df_itens_com_pedidos = df_itens_com_pedidos.merge(
        pagamentos_para_merge,
        left_on='id_pedido',
        right_on='numero_pedido',
        how='left'
    )

    df_itens_com_pedidos = df_itens_com_pedidos.drop(columns=[
        'produto_pai', 'produto_filho', 'preco_cheio', 'preco_promocional', 'preco_subtotal', 'numero_pedido'
    ])

    colunas_finais = [
        'data_criacao', 'id_pedido', 'id_cliente', 'id_produto', 'nome_produto',
        'preco_venda', 'quantidade', 'valor_envio', 'valor_subtotal', 'valor_total', 'situacao_nome', 'forma_pagamento_nome'
    ]
    colunas_presentes = [col for col in colunas_finais if col in df_itens_com_pedidos.columns]
    df_itens_com_pedidos = df_itens_com_pedidos[colunas_presentes]

    df_itens_com_pedidos = df_itens_com_pedidos.rename(columns={
        'preco_venda': 'preco_produto',
        'quantidade': 'qtd_produto',
        'valor_envio': 'valor_envio_pedido',
        'valor_subtotal': 'valor_subtotal_pedido',
        'valor_total': 'valor_total_pedido',
        'data_criacao': 'data_pedido',
        'situacao_nome': 'situacao_pedido',
        'forma_pagamento_nome': 'forma_pagamento'
    })

    with pd.ExcelWriter("dados_loja_limpo.xlsx", engine="openpyxl") as writer:
        df_clientes.to_excel(writer, sheet_name="Clientes", index=False)
        produtos_df.to_excel(writer, sheet_name="Produtos", index=False)
        df_itens_com_pedidos.to_excel(writer, sheet_name="Pedidos", index=False)

    print("\n Arquivo 'dados_loja_limpo.xlsx' gerado com sucesso.")
    
if __name__ == "__main__":
    asyncio.run(main())