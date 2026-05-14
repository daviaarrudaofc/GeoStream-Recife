"""Updated main CLI for GeoStream."""
from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor
from src.analysis import GeoAnalyzer
from src.visualizer import Visualizer
from src.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """Main entry point - runs full processing pipeline."""
    print("🚀 GeoStream - Advanced Geospatial Analysis")
    print("=" * 50)
    
    try:
        # Step 1: Load data
        print("\n📥 Loading data...")
        loader = DataLoader()
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        print(f"✅ Loaded {len(df)} facilities")
        print(f"📄 Dataset source: {loader.csv_path}")
        
        # Step 2: Process hexagons
        print("\n🔷 Processing H3 hexagons...")
        processor = GeoProcessor(loader.get_connection())
        df_h3 = processor.process_hexagons()
        gdf = processor.create_geodataframe(df_h3)
        print(f"✅ Created {len(df_h3)} hexagons")
        
        # Step 3: Analyze
        print("\n📊 Running analysis...")
        analyzer = GeoAnalyzer(loader.get_connection())
        stats = analyzer.density_statistics()
        print(f"   Mean density: {stats['avg_density']:.2f}")
        print(f"   Max density: {stats['max_density']}")
        
        clustering = analyzer.clustering_analysis(n_clusters=3)
        print(f"   Clustering inertia: {clustering['inertia']:.2f}")
        
        # Step 4: Visualize
        print("\n🎨 Creating visualizations...")
        visualizer = Visualizer()
        
        # Main choropleth map
        fig = visualizer.create_choropleth_map(
            gdf,
            output_file=visualizer.OUTPUT_DIR / "choropleth_map.html"
        )
        
        # Cluster map
        cluster_fig = visualizer.create_cluster_map(
            clustering["dataframe"],
            centers=clustering["centers"],
            output_file=visualizer.OUTPUT_DIR / "cluster_map.html"
        )
        
        print("✅ Visualizations saved to outputs/")
        
        print("\n" + "=" * 50)
        print("✨ GeoStream processing complete!")
        print("\n📂 Output files:")
        print("   - outputs/choropleth_map.html")
        print("   - outputs/cluster_map.html")
        print("\n🚀 To start the API:")
        print("   python -m uvicorn api:app --reload")
        print("\n📖 API docs available at http://localhost:8000/docs")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
