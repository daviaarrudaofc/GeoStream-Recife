# GeoStream Recife - Technical Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      FastAPI REST Server                         │
│                    (http://localhost:8000)                       │
├─────────────────────────────────────────────────────────────────┤
│                          api.py                                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │
│  │  Data Layer  │  │   GEO Layer   │  │ Analysis Lyr │            │
│  │              │  │               │  │              │            │
│  │ DataLoader   │  │ GeoProcessor  │  │ GeoAnalyzer  │            │
│  │   ↓↓↓        │  │   ↓↓↓         │  │   ↓↓↓        │            │
│  │  DuckDB      │  │   H3 Hexagon  │  │  K-means     │            │
│  │  (in-memory) │  │   GeoPandas   │  │  Clustering  │            │
│  │              │  │   Shapely     │  │              │            │
│  └──────────────┘  └──────────────┘  └──────────────┘            │
│                                                                   │
│  ┌──────────────────────────────────────┐                        │
│  │         Visualizer (Plotly)          │                        │
│  │  (Choropleth, Scatter, Cluster Maps) │                        │
│  └──────────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
         ↓              ↓              ↓
   CSV Data        HTML Maps     JSON Responses
```

## Module Architecture

### `src/config.py`
**Purpose**: Centralized configuration management

```python
GeoConfig()   # Geographic constants (H3 resolution, Recife bounds)
APIConfig()   # API settings (host, port, reload)
DataConfig()  # Data paths and CSV settings
```

### `src/logger.py`
**Purpose**: Structured logging with file + console output

```
Logs → /logs/module.log (file)
     → stdout (console)
```

### `src/data_loader.py`
**Purpose**: Data ingestion and validation

```
CSV File → read_csv_auto() → DataFrame → DuckDB (in-memory)
                             ↓
                        Validation
                             ↓
                        Type Conversion
```

### `src/geo_processor.py`
**Purpose**: Geospatial processing with H3

```
Points (lat, lon)
    ↓
H3 Hexagon Conversion (to_h3 SQL function)
    ↓
Hexagon Aggregation (GROUP BY hex_id)
    ↓
Polygon Generation (shapely.geometry)
    ↓
GeoDataFrame Creation
    ↓
Plotly Visualization
```

### `src/analysis.py`
**Purpose**: Advanced spatial analytics

```
┌────────────────────────────────────┐
│     GeoAnalyzer Methods            │
├────────────────────────────────────┤
│ • clustering_analysis()            │ → K-means (sklearn)
│ • density_statistics()             │ → SQL aggregation
│ • accessibility_index()            │ → Distance calculation
│ • coverage_analysis()              │ → Area computation
└────────────────────────────────────┘
```

### `src/visualizer.py`
**Purpose**: Interactive visualizations

```
GeoDataFrame → Plotly Express
    ↓
├─ choropleth_map()     → Density heatmap
├─ scatter_map()        → Point distribution
├─ cluster_map()        → Cluster visualization
└─ Output files (HTML)
```

## Data Flow

### Scenario 1: Full Pipeline

```
1. DataLoader.create_sample_csv()        │ Create dados_recife.csv
2. DataLoader.load_data()                │ CSV → DataFrame
3. DataLoader.setup_duckdb_table()       │ DataFrame → DuckDB
4. GeoProcessor.register_h3_function()   │ Register to_h3() in SQL
5. GeoProcessor.process_hexagons()       │ GROUP BY hex, COUNT
6. GeoProcessor.create_geodataframe()    │ Add geometry
7. Visualizer.create_choropleth_map()    │ Plotly rendering
8. visualizer.show()                     │ Browser display
```

### Scenario 2: REST API Request

```
HTTP GET /clustering?n_clusters=3
    ↓
api.get_clustering()
    ↓
analyzer.clustering_analysis(n_clusters=3)
    ↓
sklearn.cluster.KMeans
    ↓
JSON Response
    ↓
HTTP 200 with results
```

### Scenario 3: Accessibility Analysis

```
HTTP GET /accessibility?max_distance_km=2.0
    ↓
analyzer.accessibility_index()
    ↓
Haversine Distance SQL Query
    ↓
COUNT(nearby_facilities) per location
    ↓
DataFrame → JSON
    ↓
HTTP 200 response
```

## Technology Stack

### Core Processing
| Component | Library | Version | Purpose |
|-----------|---------|---------|---------|
| Geospatial | H3 | 3.7.0 | Hex indexing |
| | GeoPandas | 0.14.1 | Geometry + attributes |
| | Shapely | 2.0.2 | Geometric operations |
| SQL | DuckDB | 1.0.0 | In-memory analytics |
| Data | pandas | 2.2.0 | Data manipulation |
| ML | scikit-learn | 1.4.1 | K-means clustering |

### API & Visualization
| Component | Library | Version | Purpose |
|-----------|---------|---------|---------|
| API | FastAPI | 0.109.0 | REST framework |
| Server | Uvicorn | 0.27.0 | ASGI server |
| Viz | Plotly | 5.18.0 | Interactive maps |
| CLI | Click | 8.1.7 | Command interface |
| Validation | Pydantic | 2.5.3 | Data validation |

### Development & Deployment
| Component | Library | Version | Purpose |
|-----------|---------|---------|---------|
| Testing | pytest | 7.4.4 | Test framework |
| Coverage | pytest-cov | 4.1.0 | Coverage reports |
| Linting | flake8 | 7.0.0 | Code quality |
| Formatting | black | 24.1.1 | Code formatting |
| Import sorting | isort | 5.13.2 | Import ordering |
| Security | bandit | 1.7.5 | Security scanning |
| Containers | Docker | - | Containerization |
| Orchestration | docker-compose | 1.29+ | Multi-container |

## Database Design

### DuckDB Table Structure

```sql
CREATE TABLE pontos (
    nome VARCHAR,           -- Facility name
    lat DOUBLE,             -- Latitude
    lon DOUBLE              -- Longitude
)
```

### H3 Processing Query

```sql
SELECT 
    to_h3(lat, lon) as hex_id,           -- H3 cell identifier
    count(*) as densidade,                 -- Number of facilities
    count(DISTINCT nome) as n_equipamentos, -- Unique facilities
    array_agg(nome) as equipamentos        -- Facility names list
FROM pontos
GROUP BY 1
ORDER BY densidade DESC
```

## API Endpoint Structure

```
GET /                           # Welcome page
GET /docs                       # Swagger UI
GET /redoc                      # ReDoc UI
GET /health                     # Health check

# Facility endpoints
GET /facilities                 # All facilities
GET /facilities/count          # Facility count

# Hexagon endpoints
GET /hexagons                  # All hexagons with density
GET /hexagon/{hex_id}          # Single hexagon info

# Analysis endpoints
GET /statistics                # Density statistics
GET /clustering?n_clusters=N   # K-means results
GET /accessibility?max_distance_km=X  # Accessibility index
```

### Request/Response Examples

#### Get All Facilities
```
Request:  GET /facilities
Response: 200 OK
[
  {
    "nome": "Escola Municipal Boa Viagem",
    "latitude": -8.1256,
    "longitude": -34.9011
  },
  ...
]
```

#### Clustering Analysis
```
Request:  GET /clustering?n_clusters=3
Response: 200 OK
{
  "n_clusters": 3,
  "inertia": 0.234,
  "cluster_sizes": {0: 2, 1: 3, 2: 2},
  "centers": [[-8.05, -34.90], [...], [...]]
}
```

## Performance Characteristics

### Computational Complexity

| Operation | Algorithm | Complexity | Time |
|-----------|-----------|-----------|------|
| Data Load | Read CSV | O(n) | ~100ms |
| H3 Processing | Grouping | O(n log n) | ~50ms |
| K-means | Lloyd's Algorithm | O(nkd) | ~200ms |
| Accessibility | Haversine | O(n²) | ~500ms |
| Visualization | Render | O(n) | <100ms |

### Memory Usage

```
DuckDB Table: ~1MB (100 facilities)
GeoDataFrame: ~5MB (with geometry)
K-means Model: <1MB
Visualizations: ~50KB (HTML)
```

## Security Architecture

### Input Validation
```python
# Pydantic models validate all API inputs
class Facility(BaseModel):
    nome: str
    latitude: float
    longitude: float
```

### Error Handling
```python
@app.get("/clustering")
async def get_clustering(n_clusters: int = Query(3, ge=2, le=10)):
    # ge=2, le=10 enforces constraints
```

### Logging
```python
logger.info("Loading data...")      # Debug
logger.warning("Slow query")        # Alert
logger.error("Failed to connect")   # Error
```

## Deployment Architectures

### Single Container
```
Your Code → Docker Image → Container → API:8000
```

### Docker Compose Stack
```
API Container    (port 8000)
Redis Container  (port 6379)
Shared volumes   (data, logs, outputs)
```

### Kubernetes (Scalable)
```
Load Balancer
    ↓
Pod Replica 1 (geostream:latest)
Pod Replica 2 (geostream:latest)
Pod Replica 3 (geostream:latest)
    ↓
Persistent Volume (data)
```

## CI/CD Pipeline

```
GitHub Push
    ↓
Trigger GitHub Actions
    ├─ Lint (flake8, black, isort)
    ├─ Test (pytest on 3.9, 3.10, 3.11)
    ├─ Coverage (target >80%)
    ├─ Security (bandit scan)
    └─ Build Docker Image
    ↓
If all pass → Ready for deployment
If any fail → Notify developer
```

## Type System

### Type Hints Throughout

```python
def process_hexagons(self) -> pd.DataFrame:
    """
    Process points into H3 hexagons with density.
    
    Returns:
        DataFrame with hex_id, densidade columns
    """

def get_hex_center(self, hex_id: str) -> Tuple[float, float]:
    """
    Get center coordinates of a hexagon.
    
    Args:
        hex_id: H3 hexagon identifier
        
    Returns:
        Tuple of (latitude, longitude)
    """
```

## Logging Architecture

```
┌──────────────────────┐
│   setup_logger()     │
└──────┬───────────────┘
       ├─→ File Handler (DEBUG level)
       │   └─→ /logs/module.log
       │
       └─→ Console Handler (INFO level)
           └─→ stdout
```

## Testing Strategy

```
┌─────────────────────────────────────┐
│  test_core.py                        │
├─────────────────────────────────────┤
│ • TestDataLoader (data I/O)         │
│ • TestGeoProcessor (H3 processing)  │
│ • TestAnalyzer (analytics)          │
└─────────────────────────────────────┘
        ↓
Coverage Report (→ 80% target)
```

---

**Architecture by Design Principles**:
- ✅ Separation of Concerns
- ✅ Single Responsibility
- ✅ Dependency Injection
- ✅ Type Safety
- ✅ Comprehensive Logging
- ✅ Testability
- ✅ Scalability
