# GeoStream Recife 🌍

Advanced geospatial analysis platform for Recife public facilities using H3 hexagons, DuckDB, and interactive visualizations.

## 🎯 Features

- **H3 Hexagon Analysis**: Spatial clustering using Uber's H3 geospatial indexing
- **DuckDB Integration**: High-performance in-memory SQL queries
- **REST API**: FastAPI endpoints for programmatic access
- **Advanced Analytics**:
  - K-means clustering analysis
  - Accessibility index calculation
  - Density statistics
- **Interactive Maps**: Plotly-based choropleth and scatter visualizations
- **Docker Support**: Containerized deployment with docker-compose
- **CI/CD Pipeline**: GitHub Actions for automated testing and security scans
- **CLI Tools**: Command-line interface for data processing

## 📋 Prerequisites

- Python 3.9+
- Git
- Docker & Docker Compose (optional)

## 🚀 Quick Start

### Option 1: Local Development

```bash
# Clone repository
git clone https://github.com/daviaarrudaofc/GeoStream-Recife.git
cd GeoStream-Recife

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run main processing
python main.py

# Start API server
python -m uvicorn api:app --reload
```

### Option 2: Docker

```bash
# Build and run with docker-compose
docker-compose up

# API will be available at http://localhost:8000
```

## 📡 API Usage

### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Key Endpoints

```bash
# Get all facilities
curl http://localhost:8000/facilities

# Get facility count
curl http://localhost:8000/facilities/count

# Get hexagon analysis
curl http://localhost:8000/hexagons

# Get density statistics
curl http://localhost:8000/statistics

# Run clustering analysis
curl "http://localhost:8000/clustering?n_clusters=3"

# Get accessibility index
curl "http://localhost:8000/accessibility?max_distance_km=2.0"

# Health check
curl http://localhost:8000/health
```

## 🛠️ CLI Commands

```bash
# Show dataset information
python cli.py info

# Process data and generate visualizations
python cli.py process

# Run clustering analysis
python cli.py cluster --n-clusters 3

# Run complete analysis suite
python cli.py analyze

# Start API server
python cli.py api
```

## 📊 Architecture

```
GeoStream-Recife/
├── src/
│   ├── config.py              # Configuration settings
│   ├── logger.py              # Logging setup
│   ├── data_loader.py         # Data loading & validation
│   ├── geo_processor.py       # H3 & geometric processing
│   ├── analysis.py            # Advanced analytics (clustering, accessibility)
│   └── visualizer.py          # Plotly visualizations
├── api.py                     # FastAPI application
├── cli.py                     # CLI interface
├── main.py                    # Main entry point
├── tests/                     # Unit tests
├── Dockerfile                 # Docker image
├── docker-compose.yml         # Docker stack
└── requirements.txt           # Python dependencies
```

## 🔧 Configuration

Edit `src/config.py` to customize:

```python
GEO = GeoConfig()           # Geographic settings
API = APIConfig()           # API configuration
DATA = DataConfig()         # Data paths and settings
```

### Environment Variables

Create `.env` file:

```env
LOG_LEVEL=info
API_HOST=0.0.0.0
API_PORT=8000
REDIS_URL=redis://localhost:6379
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test
pytest tests/test_core.py::TestDataLoader -v
```

## 📈 Analysis Examples

### Clustering Analysis
```python
from src.data_loader import DataLoader
from src.analysis import GeoAnalyzer

loader = DataLoader()
df = loader.load_data()
loader.setup_duckdb_table(df)

analyzer = GeoAnalyzer(loader.get_connection())
result = analyzer.clustering_analysis(n_clusters=3)
print(f"Inertia: {result['inertia']}")
print(f"Clusters: {result['cluster_sizes']}")
```

### Accessibility Index
```python
accessibility = analyzer.accessibility_index(max_distance_km=2.0)
print(accessibility)
```

## 🐳 Docker Deployment

### Build Image
```bash
docker build -t geostream:latest .
```

### Run Container
```bash
docker run -p 8000:8000 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/outputs:/app/outputs \
  geostream:latest
```

### With Docker Compose (Recommended)
```bash
docker-compose up -d

# View logs
docker-compose logs -f geostream-api

# Stop
docker-compose down
```

## 📊 Performance

- **Data Loading**: ~100ms for 100 facilities
- **H3 Processing**: ~50ms for 100 points
- **Clustering**: ~200ms for K-means (n_clusters=3)
- **API Response**: <500ms for most endpoints

## 🔒 Security

- Type hints for better code quality
- Input validation with Pydantic
- Error handling and logging
- Docker security scanning via GitHub Actions
- Bandit security audit in CI/CD

## 🚀 CI/CD Pipeline

GitHub Actions automatically:
- ✅ Runs tests on Python 3.9, 3.10, 3.11
- 🔍 Performs code linting (flake8, black, isort)
- 📊 Generates coverage reports
- 🐳 Builds Docker images
- 🔒 Scans for security vulnerabilities (Bandit)
- ⚡ Checks performance metrics

## 📚 Technologies

| Component | Technology |
|-----------|-----------|
| Geospatial | H3, GeoPandas, Shapely |
| Database | DuckDB |
| API | FastAPI, Uvicorn |
| Analytics | scikit-learn, pandas |
| Visualization | Plotly |
| CLI | Click |
| Testing | pytest |
| Containerization | Docker |
| CI/CD | GitHub Actions |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see LICENSE file for details.

## 📧 Contact

- GitHub: [@daviaarrudaofc](https://github.com/daviaarrudaofc)
- Email: seu-email@example.com

## 🙏 Acknowledgments

- Uber H3 for hexagonal hierarchical geospatial indexing
- DuckDB for ultra-fast SQL analytics
- FastAPI for modern API development
- GeoPandas for geospatial operations

---

**Made with ❤️ in Recife**
