"""
GeoStream Recife - Legacy entry point

This file maintains backwards compatibility while using the new modular architecture.
For new development, use main.py or api.py instead.
"""

from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor
from src.visualizer import Visualizer
from src.logger import setup_logger

logger = setup_logger(__name__)

print("🚀 GeoStream Recife - Legacy Mode")
print("=" * 50)

try:
    # Load data
    print("\n📥 Loading data...")
    loader = DataLoader()
    df = loader.load_data()
    loader.setup_duckdb_table(df)
    
    # Process hexagons
    print("\n🔷 Processing H3 hexagons...")
    processor = GeoProcessor(loader.get_connection())
    df_h3 = processor.process_hexagons()
    gdf = processor.create_geodataframe(df_h3)
    
    # Visualize
    print("\n🎨 Creating visualization...")
    visualizer = Visualizer()
    fig = visualizer.create_choropleth_map(
        gdf,
        title="Densidade de Equipamentos Públicos - GeoStream Recife"
    )
    
    print("\n✅ Success! Opening map in browser...")
    visualizer.show(fig)
    
except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    print(f"\n❌ Error: {e}")