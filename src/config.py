"""Configuration settings for GeoStream."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"

# Criar diretórios se não existirem
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)


@dataclass
class GeoConfig:
    """Geographic configuration constants."""
    H3_RESOLUTION: int = 8  # Bairro-level resolution
    RECIFE_LAT: float = -8.05
    RECIFE_LON: float = -34.90
    DEFAULT_ZOOM: int = 11
    DEFAULT_COLORSCALE: str = "Viridis"
    
    # Bounds for Recife metropolitan area
    MIN_LAT: float = -8.25
    MAX_LAT: float = -7.85
    MIN_LON: float = -35.10
    MAX_LON: float = -34.70


@dataclass
class APIConfig:
    """API configuration."""
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    RELOAD: bool = True
    LOG_LEVEL: str = "info"


@dataclass
class DataConfig:
    """Data processing configuration."""
    CSV_PATH: Optional[Path] = None
    CSV_SEPARATOR: str = ";"
    ENCODING: str = "utf-8"
    
    def __post_init__(self):
        root_csv = PROJECT_ROOT / "dados_recife.csv"
        data_csv = DATA_DIR / "dados_recife.csv"

        if self.CSV_PATH is None:
            if root_csv.exists():
                self.CSV_PATH = root_csv
            else:
                self.CSV_PATH = data_csv

        if self.CSV_PATH == data_csv and root_csv.exists():
            self.CSV_PATH = root_csv


# Default configurations
GEO = GeoConfig()
API = APIConfig()
DATA = DataConfig()
