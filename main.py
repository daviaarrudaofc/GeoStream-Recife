"""Updated main CLI for GeoStream."""

from src.analysis import GeoAnalyzer
from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor
from src.logger import setup_logger
from src.visualizer import Visualizer

logger = setup_logger(__name__)


def main():
    """Main entry point - runs full processing pipeline."""
    print("GeoStream - Advanced Geospatial Analysis")
    print("=" * 50)

    try:
        # Step 1: Load data
        print("\nLoading data...")
        loader = DataLoader()
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        print(f"OK: Loaded {len(df)} facilities")
        print(f"Dataset source: {loader.csv_path}")

        # Step 2: Process hexagons
        print("\nProcessing H3 hexagons...")
        processor = GeoProcessor(loader.get_connection())
        df_h3 = processor.process_hexagons()
        gdf = processor.create_geodataframe(df_h3)
        print(f"OK: Created {len(df_h3)} hexagons")

        # Step 3: Analyze
        print("\nRunning analysis...")
        analyzer = GeoAnalyzer(loader.get_connection())
        stats = analyzer.density_statistics()
        print(f"   Mean density: {stats['avg_density']:.2f}")
        print(f"   Max density: {stats['max_density']}")

        clustering = analyzer.clustering_analysis(n_clusters=3)
        print(f"   Clustering inertia: {clustering['inertia']:.2f}")

        # Step 4: Visualize
        print("\nCreating visualizations...")
        visualizer = Visualizer()

        # Main choropleth map
        fig = visualizer.create_choropleth_map(
            gdf, output_file=visualizer.OUTPUT_DIR / "choropleth_map.html"
        )

        # Cluster map
        cluster_fig = visualizer.create_cluster_map(
            clustering["dataframe"],
            centers=clustering["centers"],
            output_file=visualizer.OUTPUT_DIR / "cluster_map.html",
        )

        print("OK: Visualizations saved to outputs/")

        print("\n" + "=" * 50)
        print("GeoStream processing complete!")
        print("\nOutput files:")
        print("   - outputs/choropleth_map.html")
        print("   - outputs/cluster_map.html")
        print("\nTo start the API:")
        print("   python -m uvicorn api:app --reload")
        print("\nAPI docs available at http://localhost:8000/docs")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    main()
