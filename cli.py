"""CLI interface for GeoStream."""
import click
from pathlib import Path
from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor
from src.analysis import GeoAnalyzer
from src.visualizer import Visualizer
from src.logger import setup_logger

logger = setup_logger(__name__)


@click.group()
def cli():
    """GeoStream - Advanced Geospatial Analysis Tool for Recife."""
    pass


@cli.command()
@click.option('--csv-path', type=click.Path(), help='Path to CSV file')
def process(csv_path):
    """Process geospatial data and generate visualizations."""
    click.echo("🚀 Starting GeoStream processing...")
    
    try:
        # Load data
        click.echo("📥 Loading data...")
        loader = DataLoader(Path(csv_path) if csv_path else None)
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        
        # Process hexagons
        click.echo("🔷 Processing H3 hexagons...")
        processor = GeoProcessor(loader.get_connection())
        df_h3 = processor.process_hexagons()
        gdf = processor.create_geodataframe(df_h3)
        
        # Visualize
        click.echo("🎨 Creating visualizations...")
        visualizer = Visualizer()
        fig = visualizer.create_choropleth_map(
            gdf,
            output_file=Path("outputs/choropleth_map.html")
        )
        
        # Show map
        visualizer.show(fig)
        
        click.echo(f"✅ Success! Processed {len(df_h3)} hexagons")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--csv-path', type=click.Path(), help='Path to CSV file')
@click.option('--n-clusters', default=3, type=int, help='Number of clusters')
def cluster(csv_path, n_clusters):
    """Run clustering analysis."""
    click.echo("🚀 Starting clustering analysis...")
    
    try:
        # Load data
        loader = DataLoader(Path(csv_path) if csv_path else None)
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        
        # Analyze
        click.echo(f"🎯 Running K-means with {n_clusters} clusters...")
        analyzer = GeoAnalyzer(loader.get_connection())
        result = analyzer.clustering_analysis(n_clusters=n_clusters)
        
        # Visualize
        click.echo("🎨 Creating cluster map...")
        visualizer = Visualizer()
        fig = visualizer.create_cluster_map(
            result["dataframe"],
            output_file=Path("outputs/cluster_map.html")
        )
        
        visualizer.show(fig)
        
        click.echo(f"✅ Clustering complete. Inertia: {result['inertia']:.2f}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--csv-path', type=click.Path(), help='Path to CSV file')
def analyze(csv_path):
    """Run full analysis suite."""
    click.echo("🚀 Starting full analysis...")
    
    try:
        # Load data
        loader = DataLoader(Path(csv_path) if csv_path else None)
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        
        # Process
        processor = GeoProcessor(loader.get_connection())
        analyzer = GeoAnalyzer(loader.get_connection())
        
        click.echo("📊 Computing statistics...")
        stats = analyzer.density_statistics()
        click.echo(f"   Total: {stats['total_facilities']}")
        click.echo(f"   Avg Density: {stats['avg_density']:.2f}")
        click.echo(f"   Max Density: {stats['max_density']}")
        
        click.echo("🎯 Computing accessibility...")
        accessibility = analyzer.accessibility_index()
        click.echo(f"   Analyzed {len(accessibility)} locations")
        
        click.echo("✅ Analysis complete!")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
def api():
    """Start API server."""
    import uvicorn
    from src.config import API
    
    click.echo(f"🚀 Starting API on {API.HOST}:{API.PORT}")
    uvicorn.run(
        "api:app",
        host=API.HOST,
        port=API.PORT,
        reload=API.RELOAD
    )


@cli.command()
@click.option('--csv-path', type=click.Path(), help='Path to CSV file')
def info(csv_path):
    """Show dataset information."""
    try:
        loader = DataLoader(Path(csv_path) if csv_path else None)
        df = loader.load_data()
        
        click.echo(f"\n📋 Dataset Information")
        click.echo(f"   Total Facilities: {len(df)}")
        click.echo(f"   Latitude Range: {df['latitude'].min():.4f} to {df['latitude'].max():.4f}")
        click.echo(f"   Longitude Range: {df['longitude'].min():.4f} to {df['longitude'].max():.4f}")
        click.echo(f"\n🏢 Facilities:")
        for _, row in df.iterrows():
            click.echo(f"   - {row['nome']}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
