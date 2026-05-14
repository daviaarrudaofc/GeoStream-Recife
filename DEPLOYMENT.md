# Production Deployment Guide

## Pre-Deployment Checklist

- [ ] All tests passing: `pytest tests/ -v`
- [ ] Code formatted: `black src/ api.py cli.py main.py`
- [ ] Linting passed: `flake8 src/ api.py cli.py main.py`
- [ ] Security scan passed: `bandit -r src/ api.py cli.py main.py`
- [ ] Environment variables configured: `.env` file created
- [ ] Data files validated
- [ ] Docker image builds: `docker build -t geostream:latest .`

## Deployment Strategies

### Option 1: Docker Compose (Recommended for Development)

```bash
# Clone repository
git clone https://github.com/daviaarrudaofc/GeoStream-Recife.git
cd GeoStream-Recife

# Create environment file
cp .env.example .env
# Edit .env with your settings

# Deploy
docker-compose up -d

# View logs
docker-compose logs -f geostream-api

# Stop
docker-compose down
```

### Option 2: Standalone Docker

```bash
# Build image
docker build -t geostream:latest .

# Run container
docker run -d \
  --name geostream \
  -p 8000:8000 \
  -e LOG_LEVEL=info \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/outputs:/app/outputs \
  geostream:latest

# View logs
docker logs -f geostream

# Stop
docker stop geostream
docker rm geostream
```

### Option 3: Kubernetes

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: geostream-api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: geostream
  template:
    metadata:
      labels:
        app: geostream
    spec:
      containers:
      - name: api
        image: geostream:latest
        ports:
        - containerPort: 8000
        env:
        - name: LOG_LEVEL
          value: "info"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: geostream-service
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8000
  selector:
    app: geostream
```

Deploy:
```bash
kubectl apply -f deployment.yaml
kubectl get pods
kubectl logs -f deployment/geostream-api
```

### Option 4: Linux/macOS Production Server

```bash
# SSH into server
ssh user@your-server.com

# Install Python 3.11+
sudo apt-get update
sudo apt-get install python3.11 python3-pip git

# Clone and setup
git clone https://github.com/daviaarrudaofc/GeoStream-Recife.git
cd GeoStream-Recife

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create systemd service
sudo cat > /etc/systemd/system/geostream.service <<EOF
[Unit]
Description=GeoStream Recife API
After=network.target

[Service]
User=www-data
WorkingDirectory=/home/user/GeoStream-Recife
Environment="PATH=/home/user/GeoStream-Recife/venv/bin"
ExecStart=/home/user/GeoStream-Recife/venv/bin/python -m uvicorn api:app --host 0.0.0.0 --port 8000

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable geostream
sudo systemctl start geostream
sudo systemctl status geostream

# View logs
sudo journalctl -u geostream -f
```

### Option 5: AWS Deployment

#### Using AWS App Runner (Easiest)

```bash
# Push to AWS ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

docker tag geostream:latest YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/geostream:latest
docker push YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/geostream:latest

# Create App Runner service in AWS Console
# Point to ECR image URL
```

#### Using AWS ECS

```bash
# Create ECS cluster
aws ecs create-cluster --cluster-name geostream-cluster

# Create task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service
aws ecs create-service \
  --cluster geostream-cluster \
  --service-name geostream-api \
  --task-definition geostream:1 \
  --desired-count 2
```

## Environment Configuration

Create `.env`:

```env
# API
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=info

# Data
CSV_PATH=/app/data/dados_recife.csv

# Geographic
H3_RESOLUTION=8
RECIFE_LAT=-8.05
RECIFE_LON=-34.90

# Performance
MAX_WORKERS=4
```

## Performance Tuning

### Memory & CPU

```bash
# Monitor resource usage
docker stats geostream-api

# Adjust if needed
docker update --memory 2g --cpus 2 geostream-api
```

### Database Optimization

DuckDB is in-memory by default, which is optimal for this use case.

### API Optimization

- Enable compression in nginx
- Use Redis caching (optional)
- Set up CDN for outputs

## Monitoring & Logging

### Health Checks

```bash
# Manual check
curl http://localhost:8000/health

# Automated monitoring
watch -n 5 'curl -s http://localhost:8000/health | jq'
```

### Logs

```bash
# Docker
docker-compose logs geostream-api

# Systemd
journalctl -u geostream -f

# File
tail -f logs/geostream.log
```

### Metrics

```bash
# API request metrics at /metrics (if Prometheus enabled)
curl http://localhost:8000/metrics
```

## Backup & Recovery

```bash
# Backup data
docker-compose exec geostream-api tar czf \
  /app/outputs/backup-$(date +%Y%m%d).tar.gz \
  /app/data

# Restore
tar xzf backup-20260513.tar.gz -C /
```

## Security

- [ ] Enable HTTPS/SSL (use nginx reverse proxy)
- [ ] Configure firewall rules
- [ ] Set up rate limiting
- [ ] Enable authentication if needed
- [ ] Regular security updates

### HTTPS with Let's Encrypt + nginx

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }
    
    location / {
        return 301 https://$server_name$request_uri;
    }
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;
    
    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Troubleshooting

### Container won't start
```bash
docker logs geostream-api
docker inspect geostream-api
```

### High memory usage
```bash
docker stats
# Reduce H3_RESOLUTION or batch processing
```

### API timeout
```bash
# Increase timeout in docker-compose.yml
environment:
  - PYTHONUNBUFFERED=1
  - UVICORN_TIMEOUT_KEEP_ALIVE=60
```

### Data not loading
```bash
# Check file permissions
docker-compose exec geostream-api ls -la /app/data
docker-compose exec geostream-api cat /app/logs/geostream.log
```

## Scaling

### Horizontal Scaling (Multiple instances)

```yaml
# docker-compose.yml
version: '3.8'
services:
  geostream-api:
    deploy:
      replicas: 3
    # ... rest of config
```

### Using Load Balancer (nginx)

```nginx
upstream geostream {
    server geostream-api-1:8000;
    server geostream-api-2:8000;
    server geostream-api-3:8000;
}

server {
    location / {
        proxy_pass http://geostream;
    }
}
```

## Support

- Documentation: Check README-NEW.md
- Issues: GitHub Issues
- Email: support@geostream.local
