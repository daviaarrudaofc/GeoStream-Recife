# Quick Start Guide - GeoStream Recife

## ⚡ 5-Minute Setup

### Prerequisites
- Python 3.9+
- Git

### Installation

```bash
# 1. Navigate to project
cd GeoStream-Recife

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate          # macOS/Linux
# or
venv\Scripts\activate            # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Process data and generate maps
python main.py
```

Done! Maps saved to `outputs/` folder. Open `choropleth_map.html` and `cluster_map.html` in your browser to view the interactive visualizations.

## 🌐 Try the API (Recommended!)

## 🌐 Try the API (Recommended!)

```bash
# Terminal 1: Start API server
python -m uvicorn api:app --reload

# Terminal 2: Open in browser
# http://localhost:8000/docs
```

Now you have an interactive API explorer with auto-documentation! 🎉

## 📊 CLI Commands

```bash
# Show info about your data
python cli.py info

# Generate map visualizations
python cli.py process

# Run clustering analysis
python cli.py cluster

# Complete analysis suite
python cli.py analyze
```

## 🐳 Docker Setup (Even Easier!)

```bash
# One command to rule them all!
docker-compose up

# Open http://localhost:8000/docs
```

## 📖 Documentation Roadmap

1. **New to GeoStream?** → Start with README-NEW.md
2. **Upgrading from old code?** → Read MIGRATION.md
3. **Going to production?** → Check DEPLOYMENT.md
4. **Contributing code?** → See CONTRIBUTING.md
5. **Need examples?** → Look at SUMMARY.md

## 🎯 What You Can Do Now

✅ Visualize facility distribution on interactive maps
✅ Query facility data via REST API
✅ Perform K-means clustering analysis
✅ Calculate accessibility indices
✅ Get comprehensive density statistics
✅ Deploy to Docker/Kubernetes
✅ Run automated tests
✅ Scale to production

## 🆘 Troubleshooting

**"Module not found" error?**
```bash
pip install -r requirements.txt
```

**Port 8000 already in use?**
```bash
python -m uvicorn api:app --port 8001 --reload
```

**API docs not loading?**
- Make sure the server is running: `python -m uvicorn api:app --reload`
- Try http://localhost:8000/docs again

**Tests failing?**
```bash
pytest tests/ -v  # Verbose output
pytest tests/ -x  # Stop on first failure
```

## 💡 Pro Tips

### Explore Your Data
```bash
python cli.py info
```

### Monitor API
```bash
# In another terminal
curl -s http://localhost:8000/health | jq
```

### Run Tests
```bash
pytest tests/ --cov=src  # Coverage report
```

### Format Code
```bash
make format  # Auto-fix code formatting
```

## 🚀 Next Level

Ready to dive deeper?

1. **Customize analysis** → Edit `src/analysis.py`
2. **Add API endpoints** → Modify `api.py`
3. **Deploy to cloud** → Follow `DEPLOYMENT.md`
4. **Contribute features** → Check `CONTRIBUTING.md`

## 📊 Project Statistics

- **Lines of Code**: ~2000 (modular & well-documented)
- **Test Coverage**: Comprehensive unit tests
- **API Endpoints**: 10+ endpoints
- **Supported Python**: 3.9, 3.10, 3.11
- **Performance**: Sub-second API responses

## 🎓 Learning Highlights

This project demonstrates:
- ✅ Professional Python architecture
- ✅ REST API design (FastAPI)
- ✅ Geospatial analysis (H3, GeoPandas)
- ✅ Unit testing & CI/CD
- ✅ Docker containerization
- ✅ Type hints & logging
- ✅ Production-ready code

## 📞 Quick Links

- **API Docs** (when running): http://localhost:8000/docs
- **Main Docs**: README-NEW.md
- **Deployment**: DEPLOYMENT.md
- **Contributing**: CONTRIBUTING.md
- **Project Summary**: SUMMARY.md

---

## What's Your Next Step?

Choose one:

### 🎨 Explore Visualizations
```bash
python main.py
```

### 🔧 Use the API
```bash
python -m uvicorn api:app --reload
# Then open http://localhost:8000/docs
```

### 🐳 Deploy with Docker
```bash
docker-compose up
# Then open http://localhost:8000
```

### 📊 Run Analysis
```bash
python cli.py analyze
```

### 🧪 Run Tests
```bash
pytest tests/ -v
```

---

**Happy analyzing!** 🌍✨

Questions? Check the documentation or create an issue on GitHub!
