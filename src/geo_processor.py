"""Geospatial processing module using H3 and GeoPandas."""

from typing import Dict, List, Tuple

import duckdb
import geopandas as gpd
import h3
import pandas as pd
import shapely.geometry

from src.config import GEO
from src.logger import setup_logger

logger = setup_logger(__name__)


class GeoProcessor:
    """Handles H3 hexagon processing and geometric operations."""

    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection):
        """Initialize GeoProcessor with DuckDB connection."""
        self.conn = duckdb_conn
        self.register_h3_function()
        logger.info(f"GeoProcessor initialized with H3 resolution {GEO.H3_RESOLUTION}")

    def register_h3_function(self):
        """Register H3 conversion function in DuckDB."""

        def latlng_to_h3(lat: float, lon: float) -> str:
            """Convert lat/lon to H3 hex ID."""
            return h3.latlng_to_cell(lat, lon, GEO.H3_RESOLUTION)

        self.conn.create_function("to_h3", latlng_to_h3, return_type="VARCHAR")
        logger.debug("H3 function registered in DuckDB")

    def process_hexagons(self) -> pd.DataFrame:
        """Process points into H3 hexagons with density."""
        logger.info("Processing hexagons...")

        df_h3 = self.conn.execute("""
            SELECT 
                to_h3(lat, lon) as hex_id,
                count(*) as densidade,
                count(DISTINCT nome) as n_equipamentos,
                array_agg(nome) as equipamentos
            FROM pontos
            GROUP BY 1
            ORDER BY densidade DESC
        """).df()

        # Register the hexagons table in DuckDB for queries
        self.conn.execute("CREATE OR REPLACE TABLE hexagons AS SELECT * FROM df_h3")

        logger.info(f"Created {len(df_h3)} hexagons")
        return df_h3

    def hex_to_polygon(self, hex_id: str) -> shapely.geometry.Polygon:
        """Convert H3 hex ID to Shapely polygon."""
        try:
            points = h3.cell_to_boundary(hex_id)
            # cell_to_boundary returns (lat, lon), convert to (lon, lat) for Shapely
            return shapely.geometry.Polygon([(p[1], p[0]) for p in points])
        except Exception as e:
            logger.error(f"Error converting hex {hex_id}: {e}")
            raise

    def create_geodataframe(self, df_h3: pd.DataFrame) -> gpd.GeoDataFrame:
        """Create GeoDataFrame from H3 hexagons."""
        logger.info("Creating GeoDataFrame...")

        df_h3["geometry"] = df_h3["hex_id"].apply(self.hex_to_polygon)
        gdf = gpd.GeoDataFrame(df_h3, geometry="geometry", crs="EPSG:4326")

        logger.info(f"GeoDataFrame created with {len(gdf)} features")
        return gdf

    def get_hex_neighbors(self, hex_id: str, ring_size: int = 1) -> List[str]:
        """Get neighboring hexagons."""
        try:
            neighbors = h3.grid_ring(hex_id, ring_size)
            return list(neighbors)
        except Exception as e:
            logger.warning(f"Error getting neighbors for {hex_id}: {e}")
            return []

    def get_hex_center(self, hex_id: str) -> Tuple[float, float]:
        """Get center coordinates of a hexagon."""
        try:
            center = h3.cell_to_latlng(hex_id)
            return center  # (lat, lon)
        except Exception as e:
            logger.error(f"Error getting center for {hex_id}: {e}")
            raise

    def get_coverage_area(self, hex_id: str) -> float:
        """Get area of hexagon in km²."""
        try:
            area_m2 = h3.cell_area(hex_id, unit="m2")
            area_km2 = area_m2 / 1_000_000
            return area_km2
        except Exception as e:
            logger.error(f"Error getting area for {hex_id}: {e}")
            raise
