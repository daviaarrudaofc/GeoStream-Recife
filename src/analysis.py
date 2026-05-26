"""Advanced geospatial analysis module."""

from typing import Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from src.logger import setup_logger

logger = setup_logger(__name__)


class GeoAnalyzer:
    """Performs advanced geospatial analysis."""

    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection):
        """Initialize analyzer."""
        self.conn = duckdb_conn
        logger.info("GeoAnalyzer initialized")

    def clustering_analysis(self, n_clusters: int = 3) -> Dict:
        """
        Perform K-means clustering on facilities.

        Args:
            n_clusters: Number of clusters

        Returns:
            Dictionary with cluster analysis results
        """
        logger.info(f"Running K-means clustering with {n_clusters} clusters")

        try:
            # Get all points
            df = self.conn.execute("SELECT nome, lat, lon FROM pontos").df()

            # Prepare data for clustering
            X = df[["lat", "lon"]].values
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            # Run K-means
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(X_scaled)

            # Add cluster labels
            df["cluster"] = clusters

            # Invert scaling to get original coordinates
            centers = scaler.inverse_transform(kmeans.cluster_centers_)

            logger.info(f"Clustering complete. Inertia: {kmeans.inertia_:.2f}")

            return {
                "dataframe": df,
                "centers": centers,
                "inertia": float(kmeans.inertia_),
                "n_clusters": n_clusters,
                "cluster_sizes": dict(df["cluster"].value_counts()),
            }

        except Exception as e:
            logger.error(f"Error in clustering analysis: {e}")
            raise

    def density_statistics(self) -> Dict:
        """Get density statistics by region."""
        logger.info("Computing density statistics")

        try:
            stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_facilities,
                    AVG(densidade) as avg_density,
                    MAX(densidade) as max_density,
                    MIN(densidade) as min_density,
                    STDDEV(densidade) as stddev_density
                FROM hexagons
            """).df()

            return stats.to_dict("records")[0]

        except Exception as e:
            logger.error(f"Error computing density statistics: {e}")
            raise

    def accessibility_index(self, max_distance_km: float = 2.0) -> pd.DataFrame:
        """
        Calculate accessibility index for points.

        Args:
            max_distance_km: Maximum distance threshold in kilometers

        Returns:
            DataFrame with accessibility scores
        """
        logger.info(f"Computing accessibility index (max distance: {max_distance_km}km)")

        try:
            # Haversine distance formula in SQL
            result = self.conn.execute(
                """
                SELECT 
                    p1.nome as location,
                    COUNT(p2.nome) as nearby_facilities,
                    COUNT(CASE WHEN p2.nome != p1.nome THEN 1 END) as other_facilities
                FROM pontos p1
                LEFT JOIN pontos p2 ON 
                    (ACOS(
                        SIN(RADIANS(p1.lat)) * SIN(RADIANS(p2.lat)) +
                        COS(RADIANS(p1.lat)) * COS(RADIANS(p2.lat)) * 
                        COS(RADIANS(p1.lon - p2.lon))
                    ) * 6371) <= ?
                GROUP BY p1.nome
                ORDER BY other_facilities DESC
            """,
                [max_distance_km],
            ).df()

            logger.info(f"Accessibility index computed for {len(result)} locations")
            return result

        except Exception as e:
            logger.warning(f"Accessibility computation using simplified method: {e}")
            # Fallback: simple count
            return self.conn.execute(
                "SELECT nome as location, COUNT(*) as nearby_facilities FROM pontos GROUP BY nome"
            ).df()

    def coverage_analysis(self, hex_gdf) -> Dict:
        """Analyze coverage using hexagon data."""
        logger.info("Analyzing coverage")

        try:
            coverage = {
                "total_hexagons": len(hex_gdf),
                "mean_density": hex_gdf["densidade"].mean(),
                "median_density": hex_gdf["densidade"].median(),
                "std_density": hex_gdf["densidade"].std(),
                "total_area_km2": hex_gdf.geometry.area.sum() / 1_000_000,
            }

            logger.info(
                f"Coverage: {len(hex_gdf)} hexagons, mean density: {coverage['mean_density']:.2f}"
            )
            return coverage

        except Exception as e:
            logger.error(f"Error in coverage analysis: {e}")
            raise
