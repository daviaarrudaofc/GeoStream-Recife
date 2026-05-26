"""Unit tests for GeoStream."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.analysis import GeoAnalyzer
from src.data_loader import DataLoader
from src.geo_processor import GeoProcessor


class TestDataLoader:
    """Test data loading functionality."""

    @pytest.fixture
    def temp_csv(self):
        """Create temporary CSV for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("nome;latitude;longitude\n")
            f.write("Test Facility;-8.05;-34.90\n")
            f.flush()
            yield Path(f.name)
        Path(f.name).unlink()

    def test_create_sample_csv(self, temp_csv):
        """Test sample CSV creation."""
        loader = DataLoader(temp_csv)
        csv_path = loader.create_sample_csv()
        assert csv_path.exists()

    def test_load_data(self, temp_csv):
        """Test data loading."""
        loader = DataLoader(temp_csv)
        df = loader.load_data()
        assert len(df) >= 1
        assert "nome" in df.columns
        assert "latitude" in df.columns
        assert "longitude" in df.columns

    def test_setup_duckdb(self, temp_csv):
        """Test DuckDB table setup."""
        loader = DataLoader(temp_csv)
        df = loader.load_data()
        conn = loader.setup_duckdb_table(df)

        result = conn.execute("SELECT COUNT(*) FROM pontos").fetchone()
        assert result[0] >= 1


class TestGeoProcessor:
    """Test geospatial processing."""

    @pytest.fixture
    def processor(self):
        """Create processor with test data."""
        loader = DataLoader()
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        return GeoProcessor(loader.get_connection())

    def test_h3_function_registered(self, processor):
        """Test H3 function is registered."""
        result = processor.conn.execute("SELECT to_h3(-8.05, -34.90)").fetchone()
        assert result[0] is not None
        assert isinstance(result[0], str)

    def test_process_hexagons(self, processor):
        """Test hexagon processing."""
        df_h3 = processor.process_hexagons()
        assert len(df_h3) > 0
        assert "hex_id" in df_h3.columns
        assert "densidade" in df_h3.columns

    def test_hex_to_polygon(self, processor):
        """Test hex conversion to polygon."""
        hex_id = processor.conn.execute("SELECT to_h3(-8.05, -34.90)").fetchone()[0]
        poly = processor.hex_to_polygon(hex_id)
        assert poly.is_valid
        assert poly.exterior.is_ring


class TestAnalyzer:
    """Test analysis functions."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer with test data."""
        loader = DataLoader()
        df = loader.load_data()
        loader.setup_duckdb_table(df)
        processor = GeoProcessor(loader.get_connection())
        processor.process_hexagons()
        return GeoAnalyzer(loader.get_connection())

    def test_density_statistics(self, analyzer):
        """Test density statistics."""
        stats = analyzer.density_statistics()
        assert "total_facilities" in stats
        assert stats["total_facilities"] > 0

    def test_clustering_analysis(self, analyzer):
        """Test clustering analysis."""
        result = analyzer.clustering_analysis(n_clusters=2)
        assert result["n_clusters"] == 2
        assert "inertia" in result
        assert "cluster_sizes" in result

    def test_accessibility_index(self, analyzer):
        """Test accessibility index."""
        result = analyzer.accessibility_index(max_distance_km=2.0)
        assert len(result) > 0
        assert "location" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
