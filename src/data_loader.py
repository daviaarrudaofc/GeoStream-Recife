"""Data loading and validation module."""
import csv
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import duckdb
from src.logger import setup_logger
from src.config import DATA, PROJECT_ROOT

logger = setup_logger(__name__)


class DataLoader:
    """Handles data loading and database initialization."""
    
    SAMPLE_DATA = [
        ["nome", "latitude", "longitude"],
        ["Escola Municipal Boa Viagem", "-8.1256", "-34.9011"],
        ["Escola Municipal Casa Forte", "-8.0389", "-34.9152"],
        ["Escola Municipal Varzea", "-8.0471", "-34.9431"],
        ["Escola Municipal Centro", "-8.0631", "-34.8711"],
        ["Escola Municipal Derby", "-8.0575", "-34.8978"],
        ["Creche Municipal Recife", "-8.0500", "-34.8800"],
        ["Escola Tecnica", "-8.0700", "-34.9000"]
    ]
    
    def __init__(self, csv_path: Path = None):
        """Initialize DataLoader."""
        self.csv_path = csv_path or DATA.CSV_PATH
        self.conn = duckdb.connect(":memory:")
        logger.info(f"DataLoader initialized with CSV path: {self.csv_path}")
    
    def create_sample_csv(self) -> Path:
        """Create sample CSV file if it doesn't exist."""
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.csv_path.exists():
            logger.info(f"CSV file already exists: {self.csv_path}")
            return self.csv_path
        
        logger.info(f"Creating sample CSV at {self.csv_path}")
        with open(self.csv_path, 'w', newline='', encoding=DATA.ENCODING) as f:
            writer = csv.writer(f, delimiter=DATA.CSV_SEPARATOR)
            writer.writerows(self.SAMPLE_DATA)
        
        logger.info("Sample CSV created successfully")
        return self.csv_path
    
    def load_data(self) -> pd.DataFrame:
        """Load and validate data from CSV."""
        self.create_sample_csv()
        
        try:
            logger.info(f"Loading data from {self.csv_path}")
            df = pd.read_csv(
                self.csv_path,
                sep=DATA.CSV_SEPARATOR,
                encoding=DATA.ENCODING
            )
            
            # Validate columns
            required_cols = {"nome", "latitude", "longitude"}
            if not required_cols.issubset(df.columns):
                raise ValueError(f"Missing required columns. Expected: {required_cols}")
            
            # Convert to numeric and remove invalid rows
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            df = df.dropna(subset=["latitude", "longitude"])
            
            logger.info(f"Loaded {len(df)} valid records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def setup_duckdb_table(self, df: pd.DataFrame) -> duckdb.DuckDBPyConnection:
        """Create DuckDB table from DataFrame."""
        logger.info("Setting up DuckDB table...")
        
        self.conn.execute("""
            CREATE TABLE pontos AS 
            SELECT 
                nome,
                latitude as lat, 
                longitude as lon
            FROM df
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        
        row_count = self.conn.execute("SELECT COUNT(*) FROM pontos").fetchone()[0]
        logger.info(f"DuckDB table created with {row_count} rows")
        
        return self.conn
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return DuckDB connection."""
        return self.conn
