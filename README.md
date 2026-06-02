# GeoStream Recife

Plataforma de analise geoespacial para equipamentos publicos do Recife usando H3,
DuckDB, FastAPI e visualizacoes interativas.

## Recursos

- Processamento de pontos em hexagonos H3.
- Consultas analiticas em memoria com DuckDB.
- API REST com FastAPI.
- CLI para processar dados, gerar mapas e consultar informacoes.
- Analises de densidade, clusterizacao K-means e acessibilidade.
- Mapas interativos em HTML gerados com Plotly.
- Docker e GitHub Actions para validacao continua.

## Estrutura

```text
GeoStream-Recife/
|-- .github/workflows/ci-cd.yml
|-- data/dados_recife.csv
|-- infra/docker/
|   |-- Dockerfile
|   `-- docker-compose.yml
|-- runtime/                  # gerado em execucao: logs, mapas e modelos
|-- scripts/
|   |-- api.py
|   |-- cli.py
|   |-- legacy_app.py
|   `-- main.py
|-- src/
|   |-- config.py
|   |-- data_loader.py
|   |-- geo_processor.py
|   |-- analysis.py
|   |-- visualizer.py
|   `-- logger.py
|-- tests/test_core.py
|-- requirements.txt
|-- pyproject.toml
`-- Makefile
```

## Instalacao local

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

No Linux/macOS:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Uso

Rodar o pipeline completo:

```bash
python -m scripts.main
```

Usar a CLI:

```bash
python -m scripts.cli info
python -m scripts.cli process
python -m scripts.cli cluster --n-clusters 3
python -m scripts.cli analyze
python -m scripts.cli api
```

Iniciar a API diretamente:

```bash
python -m uvicorn scripts.api:app --reload
```

Depois acesse:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- Health check: http://localhost:8000/health

## Docker

```bash
docker compose -f infra/docker/docker-compose.yml up --build
```

Ou:

```bash
docker build -f infra/docker/Dockerfile -t geostream:latest .
docker run -p 8000:8000 geostream:latest
```

## Dados

O CSV oficial do projeto fica em `data/dados_recife.csv`.

Formato esperado:

```csv
nome;latitude;longitude
Escola Municipal Boa Viagem;-8.1256;-34.9011
```

Se o arquivo nao existir, o `DataLoader` cria uma amostra minima no mesmo caminho.

## Testes e qualidade

```bash
pytest tests/ -v
pytest tests/ --cov=src --cov-report=term
flake8 src scripts
black --check src scripts
isort --check-only src scripts
bandit -r src scripts
```

## CI/CD

O workflow em `.github/workflows/ci-cd.yml` executa:

- testes em multiplas versoes do Python;
- lint basico com flake8;
- checagem de formatacao com black e isort;
- cobertura de testes;
- build Docker;
- auditoria com Bandit;
- verificacao simples de performance.

As actions foram atualizadas para versoes atuais, incluindo `actions/upload-artifact@v4`,
porque a versao v3 dos artifact actions deixou de funcionar no GitHub Actions em
30 de janeiro de 2025.
