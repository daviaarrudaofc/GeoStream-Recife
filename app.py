import duckdb
import h3
import pandas as pd
import plotly.express as px
import shapely.geometry
import geopandas as gpd
import csv
import os

# --- PASSO 1: SIMULAÇÃO DE DADOS REAIS (PLANO B) ---
# Como o site oficial pode cair, criamos um CSV local com coordenadas reais de Recife
print("Preparando arquivo de dados local...")
csv_path = 'dados_recife.csv'
data = [
    ["nome", "latitude", "longitude"],
    ["Escola Municipal Boa Viagem", "-8.1256", "-34.9011"],
    ["Escola Municipal Casa Forte", "-8.0389", "-34.9152"],
    ["Escola Municipal Varzea", "-8.0471", "-34.9431"],
    ["Escola Municipal Centro", "-8.0631", "-34.8711"],
    ["Escola Municipal Derby", "-8.0575", "-34.8978"],
    ["Creche Municipal Recife", "-8.0500", "-34.8800"],
    ["Escola Tecnica", "-8.0700", "-34.9000"]
]

with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=';')
    writer.writerows(data)

# --- PASSO 2: CONFIGURAÇÃO DO DUCKDB ---
# Conectando ao banco em memória
con = duckdb.connect()

print("Carregando e limpando dados no DuckDB...")
# O DuckDB lê o CSV e já limpa a vírgula (se houver) e converte para DOUBLE
print("Carregando e limpando dados no DuckDB...")
con.execute("""
    CREATE TABLE pontos AS 
    SELECT 
        nome,
        CAST(latitude AS DOUBLE) as lat, 
        CAST(longitude AS DOUBLE) as lon
    FROM read_csv_auto('dados_recife.csv', sep=';', header=True)
    WHERE latitude IS NOT NULL
""")

# --- PASSO 3: INTEGRAÇÃO COM H3 ---
# Definindo a função de conversão
def latlng_to_h3(lat, lon):
    # Resolução 8 é excelente para visualizar bairros de Recife
    return h3.latlng_to_cell(lat, lon, 8)

# Registrando a função no SQL do DuckDB
con.create_function("to_h3", latlng_to_h3, return_type='VARCHAR')

print("Processando inteligência geográfica...")
# Agrupando os pontos por hexágono usando SQL
df_h3 = con.execute("""
    SELECT 
        to_h3(lat, lon) as hex_id,
        count(*) as densidade
    FROM pontos
    GROUP BY 1
""").df()

# --- PASSO 4: GEOPANDAS E GEOMETRIA ---
print("Gerando polígonos dos hexágonos...")
def get_poly(hex_id):
    points = h3.cell_to_boundary(hex_id)
    # GeoJSON espera (longitude, latitude)
    return shapely.geometry.Polygon([(p[1], p[0]) for p in points])

df_h3['geometry'] = df_h3['hex_id'].apply(get_poly)
gdf = gpd.GeoDataFrame(df_h3, geometry='geometry')

# --- PASSO 5: VISUALIZAÇÃO COM PLOTLY ---
print("Renderizando mapa interativo...")
fig = px.choropleth_mapbox(
    gdf,
    geojson=gdf.__geo_interface__,
    locations=gdf.index,
    color='densidade',
    color_continuous_scale="Viridis",
    mapbox_style="carto-positron",
    zoom=11,
    center={"lat": -8.05, "lon": -34.90},
    opacity=0.7,
    title="Densidade de Equipamentos Públicos - GeoStream Recife",
    labels={'densidade': 'Qtd Escolas'}
)

# Ajuste fino do layout
fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})

print("Sucesso! Abrindo mapa no navegador...")
fig.show()