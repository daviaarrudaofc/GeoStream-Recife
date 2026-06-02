"""Configuration settings for GeoStream."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
MODELS_DIR = RUNTIME_DIR / "models"
OUTPUT_DIR = RUNTIME_DIR / "outputs"

# Create runtime directories when the package is imported.
DATA_DIR.mkdir(exist_ok=True)
RUNTIME_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


@dataclass
class GeoConfig:
    """Geographic configuration constants."""

    H3_RESOLUTION: int = 8
    RECIFE_LAT: float = -8.05
    RECIFE_LON: float = -34.90
    DEFAULT_ZOOM: int = 11
    DEFAULT_COLORSCALE: str = "Viridis"

    MIN_LAT: float = -8.25
    MAX_LAT: float = -7.85
    MIN_LON: float = -35.10
    MAX_LON: float = -34.70


@dataclass
class APIConfig:
    """API configuration."""

    # Required for Docker and external API access.
    HOST: str = "0.0.0.0"  # nosec B104
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
        if self.CSV_PATH is None:
            self.CSV_PATH = DATA_DIR / "dados_recife.csv"


GEO = GeoConfig()
API = APIConfig()
DATA = DataConfig()
