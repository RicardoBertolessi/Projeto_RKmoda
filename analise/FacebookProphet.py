import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
from prophet import Prophet
from sqlalchemy import create_engine
import pandas as pd
from matplotlib.ticker import MultipleLocator

# Conexão com o banco de dados
usuario = 'postgres'
senha   = '12345678'
host    = 'localhost'
porta   = '5432'
banco   = 'rk moda'

engine = create_engine(
    f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}'
)

# Consulta SQL para trazer a receita mensal
query = """
SELECT
    EXTRACT(YEAR FROM data_pedido::date)  AS ano,
    EXTRACT(MONTH FROM data_pedido::date) AS mes,
    SUM(valor_subtotal_pedido::decimal) AS receita_liquida
FROM itens_pedidos
GROUP BY ano, mes
ORDER BY ano, mes;
"""

# Lê os dados em um DataFrame
df = pd.read_sql(query, engine)

# Converte receita para número e ajusta a escala
SCALE_FACTOR = 10  # ajuste se necessário
df['receita_liquida'] = pd.to_numeric(df['receita_liquida'], errors='coerce') / SCALE_FACTOR

# Prepara colunas para o Prophet
df['ds'] = pd.to_datetime(
    df['ano'].astype(int).astype(str) + '-' +
    df['mes'].astype(int).astype(str) + '-01'
)
df['y'] = df['receita_liquida']

# Cria e treina o modelo de previsão
modelo = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False
)
modelo.fit(df[['ds', 'y']])

# Gera previsão para os próximos 6 meses
futuro = modelo.make_future_dataframe(periods=6, freq='M')
previsao = modelo.predict(futuro)

# Cria gráfico com histórico e previsão
fig, ax = plt.subplots(figsize=(16, 8))
ax.plot(df['ds'], df['y'], label='Histórico', marker='o')
ax.plot(previsao['ds'], previsao['yhat'], label='Previsão')
ax.fill_between(previsao['ds'], previsao['yhat_lower'], previsao['yhat_upper'],
                alpha=0.3, label='Intervalo de confiança')

# Formatação do eixo Y
ax.ticklabel_format(style='plain', axis='y')
ax.yaxis.set_major_formatter(mtick.StrMethodFormatter('R$ {x:,.0f}'))
ax.yaxis.set_major_locator(MultipleLocator(10_000))

# Define limite do eixo Y
y_max = max(df['y'].max(), previsao['yhat'].max())
ax.set_ylim(0, 10_000 * (int(y_max / 10_000) + 1))

# Formatação do eixo X
locator = mdates.MonthLocator(bymonth=(1, 7))
ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b/%Y'))
plt.xticks(rotation=45)

# Títulos e rótulos
ax.set_xlim(df['ds'].min(), previsao['ds'].max())
ax.set_xlabel('Data', fontsize=14)
ax.set_ylabel('Receita Líquida', fontsize=14)
ax.set_title('Previsão de Receita Líquida - Próximos 6 Meses', fontsize=16)
ax.legend()
ax.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Exibe os componentes da previsão
modelo.plot_components(previsao)
plt.show()

# Renomeia colunas para português antes de salvar no banco
df_previsao = previsao[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].rename(columns={
    'ds': 'data',
    'yhat': 'previsao_receita',
    'yhat_lower': 'intervalo_inferior',
    'yhat_upper': 'intervalo_superior'
})

# Grava os dados da previsão no banco de dados
df_previsao.to_sql(
    name="tabela_previsao",
    con=engine,
    if_exists="append",
    index=False
)

print("Previsões gravadas com sucesso no Data Warehouse! (valores divididos por", SCALE_FACTOR, ")")
