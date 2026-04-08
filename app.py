import duckdb
import h3
import pandas as pd
import plotly.express as px
import shapely.geometry

# 1. Conectar ao DuckDB (em memória)
con = duckdb.connect()

# 2. Criar dados de exemplo (1000 pontos em Recife)
print("Gerando dados...")
con.execute("""
    CREATE TABLE pontos AS 
    SELECT 
        -8.05 + (random() * 0.05) AS lat, 
        -34.90 + (random() * 0.05) AS lon 
    FROM range(1000)
""")

# 3. Função para converter Lat/Lon em Hexágono H3
def latlng_to_h3(lat, lon):
    return h3.latlng_to_cell(lat, lon, 8) # Resolução 8 (~0.7km²)

# Aqui adicionamos o 'return_type'
con.create_function("to_h3", latlng_to_h3, return_type='VARCHAR')

# 4. Agregação ultra rápida com SQL
print("Processando hexágonos...")
df_h3 = con.execute("""
    SELECT 
        to_h3(lat, lon) as hex_id,
        count(*) as densidade
    FROM pontos
    GROUP BY 1
""").df()

# 5. Criar Geometria real com GeoPandas
def get_poly(hex_id):
    points = h3.cell_to_boundary(hex_id)
    # Inverte de (lat, lon) para (lon, lat) que o GeoPandas/GIS prefere
    return shapely.geometry.Polygon([(p[1], p[0]) for p in points])

df_h3['geometry'] = df_h3['hex_id'].apply(get_poly)
import geopandas as gpd
gdf = gpd.GeoDataFrame(df_h3, geometry='geometry')

# 6. Plotar usando o Mapbox do Plotly (Modo Corrigido)
fig = px.choropleth_mapbox(
    gdf,
    geojson=gdf.__geo_interface__, # Aqui está o segredo: passa a geometria para o Plotly
    locations=gdf.index,
    color='densidade',
    mapbox_style="carto-positron",
    zoom=11,
    center={"lat": -8.05, "lon": -34.90},
    opacity=0.6
)

fig.show()