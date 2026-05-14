"""FastAPI REST API for GeoStream."""
from typing import List, Optional, Dict
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
import uvicorn
from pydantic import BaseModel

from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor
from src.analysis import GeoAnalyzer
from src.visualizer import Visualizer
from src.config import API
from src.logger import setup_logger

logger = setup_logger(__name__)

# Initialize app
app = FastAPI(
    title="GeoStream Recife API",
    description="Advanced geospatial analysis API for Recife public facilities",
    version="1.0.0"
)

# Global instances
loader: Optional[DataLoader] = None
processor: Optional[GeoProcessor] = None
analyzer: Optional[GeoAnalyzer] = None
visualizer: Optional[Visualizer] = None


@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    global loader, processor, analyzer, visualizer
    logger.info("Initializing API...")
    loader = DataLoader()
    df = loader.load_data()
    loader.setup_duckdb_table(df)
    processor = GeoProcessor(loader.get_connection())
    analyzer = GeoAnalyzer(loader.get_connection())
    visualizer = Visualizer()
    logger.info("API initialized successfully")


# Pydantic models
class Facility(BaseModel):
    """Facility data model."""
    nome: str
    latitude: float
    longitude: float


class HexagonInfo(BaseModel):
    """Hexagon information model."""
    hex_id: str
    densidade: int
    equipamentos: List[str]


class ClusterAnalysisResult(BaseModel):
    """Cluster analysis result model."""
    n_clusters: int
    inertia: float
    cluster_sizes: Dict[int, int]


# Routes
@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with API documentation."""
    return """
    <html>
        <head>
            <title>GeoStream Recife API</title>
            <style>
                body { font-family: Arial; margin: 40px; background: #f5f5f5; }
                h1 { color: #333; }
                .endpoint { background: white; padding: 15px; margin: 10px 0; border-left: 4px solid #0066cc; }
                code { background: #eee; padding: 2px 6px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <h1>🌍 GeoStream Recife API</h1>
            <p>Advanced geospatial analysis for Recife public facilities</p>
            
            <h2>📍 Available Endpoints</h2>
            <div class="endpoint">
                <h3><code>GET /facilities</code></h3>
                <p>List all facilities</p>
            </div>
            <div class="endpoint">
                <h3><code>GET /hexagons</code></h3>
                <p>Get H3 hexagon analysis with density</p>
            </div>
            <div class="endpoint">
                <h3><code>GET /statistics</code></h3>
                <p>Get density statistics</p>
            </div>
            <div class="endpoint">
                <h3><code>GET /clustering</code></h3>
                <p>Get K-means clustering analysis</p>
            </div>
            <div class="endpoint">
                <h3><code>GET /accessibility</code></h3>
                <p>Get accessibility index</p>
            </div>
            <div class="endpoint">
                <h3><code>GET /docs</code></h3>
                <p>Interactive API documentation (Swagger UI)</p>
            </div>
        </body>
    </html>
    """


@app.get("/facilities", response_model=List[Facility])
async def get_facilities():
    """Get all facilities."""
    try:
        df = loader.conn.execute("SELECT nome, lat as latitude, lon as longitude FROM pontos").df()
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error fetching facilities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/facilities/count")
async def count_facilities():
    """Get facility count."""
    try:
        count = loader.conn.execute("SELECT COUNT(*) as total FROM pontos").fetchone()[0]
        return {"total_facilities": count}
    except Exception as e:
        logger.error(f"Error counting facilities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hexagons")
async def get_hexagons():
    """Get H3 hexagon density analysis."""
    try:
        df_h3 = processor.process_hexagons()
        return df_h3.to_dict('records')
    except Exception as e:
        logger.error(f"Error processing hexagons: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hexagon/{hex_id}")
async def get_hexagon_info(hex_id: str):
    """Get information about a specific hexagon."""
    try:
        lat, lon = processor.get_hex_center(hex_id)
        area = processor.get_hex_center(hex_id)
        neighbors = processor.get_hex_neighbors(hex_id)
        
        return {
            "hex_id": hex_id,
            "center": {"latitude": lat, "longitude": lon},
            "neighbors": neighbors
        }
    except Exception as e:
        logger.error(f"Error fetching hexagon info: {e}")
        raise HTTPException(status_code=404, detail=f"Hexagon not found: {hex_id}")


@app.get("/statistics")
async def get_statistics():
    """Get density statistics."""
    try:
        stats = analyzer.density_statistics()
        return stats
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/clustering")
async def get_clustering(n_clusters: int = Query(3, ge=2, le=10)):
    """
    Get K-means clustering analysis.
    
    Args:
        n_clusters: Number of clusters (2-10)
    """
    try:
        result = analyzer.clustering_analysis(n_clusters=n_clusters)
        return {
            "n_clusters": result["n_clusters"],
            "inertia": result["inertia"],
            "cluster_sizes": result["cluster_sizes"],
            "centers": result["centers"].tolist()
        }
    except Exception as e:
        logger.error(f"Error in clustering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/accessibility")
async def get_accessibility(max_distance_km: float = Query(2.0, ge=0.5, le=10.0)):
    """
    Get accessibility index.
    
    Args:
        max_distance_km: Maximum distance threshold in kilometers
    """
    try:
        result = analyzer.accessibility_index(max_distance_km=max_distance_km)
        return result.to_dict('records')
    except Exception as e:
        logger.error(f"Error computing accessibility: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "GeoStream Recife API",
        "version": "1.0.0"
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=API.HOST,
        port=API.PORT,
        reload=API.RELOAD,
        log_level=API.LOG_LEVEL
    )
