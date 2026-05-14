# 🌍 GeoStream Recife - Guia Completo em Português

## 📋 Índice
1. [O que é?](#o-que-é)
2. [Como Começar](#como-começar)
3. [3 Formas de Rodar](#3-formas-de-rodar)
4. [Usando a API](#usando-a-api)
5. [Comandos CLI](#comandos-cli)
6. [Análises Disponíveis](#análises-disponíveis)
7. [Docker](#docker)
8. [Testes](#testes)

---

## ❓ O que é?

**GeoStream Recife** é uma plataforma profissional de análise geoespacial que:

✅ Visualiza equipamentos públicos em mapas interativos
✅ Agrupa dados em hexágonos H3 (índice geoespacial)
✅ Realiza análises avançadas (clustering, acessibilidade, densidade)
✅ Oferece API REST para acesso programático
✅ Funciona com Docker para deploy fácil

---

## 🚀 Como Começar (5 Minutos)

### 1️⃣ **Instalação Rápida**

```bash
# Entre na pasta do projeto
cd GeoStream-Recife

# Crie um ambiente virtual
python -m venv venv

# Ative o ambiente
# No Windows:
venv\Scripts\activate
# No Mac/Linux:
source venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

### 2️⃣ **Primeira Execução**

Escolha UMA opção abaixo:

---

## 3️⃣ 3 Formas de Rodar

### **OPÇÃO A: Forma Simples (Recomendado para Iniciantes)**

```bash
python main.py
```

✅ Executa tudo automaticamente
✅ Abre o mapa no navegador
✅ Salva mapas em `outputs/`

---

### **OPÇÃO B: API REST (Recomendado para Desenvolvedores)**

```bash
python -m uvicorn api:app --reload
```

Depois abra no navegador:
```
http://localhost:8000/docs
```

✅ Interface interativa com documentação automática
✅ Teste os endpoints do API
✅ Veja exemplos de requisições e respostas

---

### **OPÇÃO C: Docker (Recomendado para Produção)**

```bash
# Instale Docker primeiro: https://www.docker.com/

docker-compose up
```

Depois abra:
```
http://localhost:8000
```

✅ Tudo funciona em container
✅ Não precisa instalar dependências localmente
✅ Redis cache incluído

---

## 📡 Usando a API

### Acessar Documentação Interativa

```
http://localhost:8000/docs
```

Você verá uma interface Swagger UI onde pode testar todos os endpoints.

### Exemplos de Requisições

#### 1. **Listar todas as escolas**
```bash
curl http://localhost:8000/facilities
```

Resposta:
```json
[
  {
    "nome": "Escola Municipal Boa Viagem",
    "latitude": -8.1256,
    "longitude": -34.9011
  },
  {
    "nome": "Escola Municipal Casa Forte",
    "latitude": -8.0389,
    "longitude": -34.9152
  }
]
```

#### 2. **Contar escolas**
```bash
curl http://localhost:8000/facilities/count
```

Resposta:
```json
{
  "total_facilities": 7
}
```

#### 3. **Obter hexágonos com densidade**
```bash
curl http://localhost:8000/hexagons
```

Resposta:
```json
[
  {
    "hex_id": "881ea6df0bfffff",
    "densidade": 3,
    "equipamentos": ["Escola A", "Escola B", "Creche"]
  }
]
```

#### 4. **Clustering (Agrupar escolas)**
```bash
curl "http://localhost:8000/clustering?n_clusters=3"
```

Resposta:
```json
{
  "n_clusters": 3,
  "inertia": 0.234,
  "cluster_sizes": {
    "0": 2,
    "1": 3,
    "2": 2
  },
  "centers": [[-8.05, -34.90], [-8.10, -34.88], [-8.01, -34.92]]
}
```

#### 5. **Acessibilidade (Escolas perto uma da outra)**
```bash
curl "http://localhost:8000/accessibility?max_distance_km=2.0"
```

Resposta:
```json
[
  {
    "location": "Escola Municipal Boa Viagem",
    "nearby_facilities": 3,
    "other_facilities": 2
  }
]
```

#### 6. **Verificar saúde da API**
```bash
curl http://localhost:8000/health
```

Resposta:
```json
{
  "status": "healthy",
  "service": "GeoStream Recife API",
  "version": "1.0.0"
}
```

---

## 🛠️ Comandos CLI

### **Opção D: Linha de Comando**

#### Ver informações dos dados
```bash
python cli.py info
```

Saída:
```
📋 Dataset Information
   Total Facilities: 7
   Latitude Range: -8.1256 to -8.0389
   Longitude Range: -34.9431 to -34.8711

🏢 Facilities:
   - Escola Municipal Boa Viagem
   - Escola Municipal Casa Forte
   - Escola Municipal Varzea
   - Escola Municipal Centro
   - Escola Municipal Derby
   - Creche Municipal Recife
   - Escola Tecnica
```

#### Processar dados e gerar mapas
```bash
python cli.py process
```

Gera:
- `outputs/choropleth_map.html` - Mapa com cores por densidade
- Abre automaticamente no navegador

#### Análise de clustering
```bash
python cli.py cluster --n-clusters 3
```

Gera:
- `outputs/cluster_map.html` - Mapa com cores por cluster
- Mostra qualidade do clustering

#### Análise completa
```bash
python cli.py analyze
```

Executa:
- Estatísticas de densidade
- Índice de acessibilidade
- Resumo da cobertura

#### Iniciar servidor API
```bash
python cli.py api
```

Equivale a:
```bash
python -m uvicorn api:app --reload
```

---

## 📊 Análises Disponíveis

### 1. **Hexágonos H3** 🔷
O que faz: Agrupa as escolas em hexágonos para análise espacial
```bash
curl http://localhost:8000/hexagons
```

### 2. **Estatísticas de Densidade** 📈
O que faz: Calcula média, máximo, mínimo de escolas por área
```bash
curl http://localhost:8000/statistics
```

### 3. **Clustering K-means** 🎯
O que faz: Identifica grupos naturais de escolas
```bash
curl "http://localhost:8000/clustering?n_clusters=3"
```

Personalize:
- `n_clusters=2` para 2 grupos
- `n_clusters=5` para 5 grupos
- Máximo: 10 grupos

### 4. **Índice de Acessibilidade** 🚶
O que faz: Mostra quantas escolas estão perto uma da outra
```bash
curl "http://localhost:8000/accessibility?max_distance_km=2.0"
```

Personalize a distância:
- `max_distance_km=1.0` para 1km
- `max_distance_km=3.0` para 3km
- Máximo: 10km

---

## 🐳 Docker

### Executar com Docker Compose (Recomendado)

```bash
# Iniciar
docker-compose up

# Em outro terminal, ver logs
docker-compose logs -f geostream-api

# Parar
docker-compose down
```

### O que é incluído?
- **geostream-api** (porta 8000) - A API
- **redis** (porta 6379) - Cache (opcional)

### Acessar
```
http://localhost:8000/docs
```

---

## 🧪 Testes

### Rodar todos os testes
```bash
pytest tests/ -v
```

Saída:
```
tests/test_core.py::TestDataLoader::test_load_data PASSED
tests/test_core.py::TestGeoProcessor::test_process_hexagons PASSED
tests/test_core.py::TestAnalyzer::test_clustering_analysis PASSED
```

### Testes com cobertura
```bash
pytest tests/ --cov=src --cov-report=html
```

Depois abra:
```
htmlcov/index.html
```

### Rodar teste específico
```bash
pytest tests/test_core.py::TestDataLoader -v
```

---

## 🧹 Manutenção

### Limpar arquivos temporários
```bash
make clean
```

### Formatar código
```bash
make format
```

### Verificar qualidade do código
```bash
make lint
```

### Tudo de uma vez
```bash
make all
```

---

## 📁 Arquivos Importantes

| Arquivo | Uso |
|---------|-----|
| `src/` | Código principal |
| `api.py` | Servidor REST |
| `cli.py` | Interface de linha de comando |
| `main.py` | Execução simples |
| `data/dados_recife.csv` | Dados das escolas |
| `outputs/` | Mapas gerados |
| `logs/` | Arquivo de logs |

---

## ⚙️ Configuração

### Alterar configurações
Edite `src/config.py`:

```python
GEO = GeoConfig()
GEO.H3_RESOLUTION = 8          # Resolução do hexágono (6-10)
GEO.RECIFE_LAT = -8.05         # Latitude do centro
GEO.RECIFE_LON = -34.90        # Longitude do centro
GEO.DEFAULT_ZOOM = 11          # Zoom do mapa (1-20)

API = APIConfig()
API.PORT = 8000                # Porta da API
API.LOG_LEVEL = "info"         # Nível de log
```

### Usar arquivo `.env`
```bash
# Copie o exemplo
cp .env.example .env

# Edite o arquivo
nano .env

# Use com:
python main.py
```

---

## 🆘 Problemas Comuns

### "Módulo não encontrado"
```bash
# Reinstale as dependências
pip install -r requirements.txt --force-reinstall
```

### "Porta 8000 já em uso"
```bash
# Use outra porta
python -m uvicorn api:app --port 8001 --reload
```

### "Erro ao carregar CSV"
```bash
# Verificar arquivo
ls -la data/dados_recife.csv

# Se não existir, o programa cria automaticamente
python main.py
```

### "Docker não inicia"
```bash
# Ver logs de erro
docker-compose logs geostream-api

# Reconstruir imagem
docker-compose up --build
```

---

## 📊 Fluxo de Dados

```
1. dados_recife.csv (7 escolas)
        ↓
2. DataLoader (valida dados)
        ↓
3. DuckDB (banco em memória)
        ↓
4. GeoProcessor (H3 hexágonos)
        ↓
5. GeoDataFrame (adiciona geometria)
        ↓
6. Analyzer (estatísticas, clustering)
        ↓
7. Visualizer (Plotly maps)
        ↓
8. Navegador / API REST
```

---

## 🎯 Casos de Uso

### Use `python main.py` se:
- Quer apenas gerar mapas
- Não precisa de API
- Usa localmente

### Use `python -m uvicorn api:app` se:
- Quer usar como API
- Precisa consumir via HTTP
- Quer testar endpoints

### Use `docker-compose up` se:
- Quer deploy em produção
- Precisa de reprodutibilidade
- Usa em servidor

---

## 📚 Documentação Completa

- **README-NEW.md** - Guia técnico detalhado
- **QUICKSTART.md** - Setup em 5 minutos
- **DEPLOYMENT.md** - Deploy para produção
- **ARCHITECTURE.md** - Arquitetura técnica
- **API.md** - Referência de endpoints

---

## ✨ Destaques da Plataforma

✅ **Modular** - Código organizado em camadas
✅ **Rápido** - Respostas <500ms
✅ **Seguro** - Validação de inputs
✅ **Testado** - 10+ testes unitários
✅ **Documentado** - Swagger UI automática
✅ **Escalável** - Pronto para Kubernetes
✅ **Profissional** - Type hints, logging, CI/CD

---

## 🎉 Pronto para Começar?

### Escolha um comando:

**Iniciante? (Forma mais simples)**
```bash
python main.py
```

**Desenvolvedor? (Com API)**
```bash
python -m uvicorn api:app --reload
```

**DevOps? (Com Docker)**
```bash
docker-compose up
```

**Power User? (Linha de comando)**
```bash
python cli.py analyze
```

---

## 🤝 Ajuda

Encontrou problema?

1. Verifique a seção "Problemas Comuns" acima
2. Rode os testes: `pytest tests/ -v`
3. Veja os logs: `logs/geostream.log`
4. Leia a documentação: `README-NEW.md`

---

## 📧 Informações

- **Projeto**: GeoStream Recife
- **Versão**: 1.0.0
- **Python**: 3.9+
- **Licença**: MIT
- **GitHub**: [daviaarrudaofc/GeoStream-Recife](https://github.com/daviaarrudaofc/GeoStream-Recife)

---

**Feito com ❤️ em Recife** 🌴

Aproveita a análise! 🚀✨
