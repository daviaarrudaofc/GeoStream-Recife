# GeoStream Development Tasks

.PHONY: help install dev test lint format clean docker-build docker-up docker-down api cli

PYTHON_FILES = src scripts tests
DOCKER_COMPOSE = docker compose -f infra/docker/docker-compose.yml

help:
	@echo "GeoStream Recife - Available Commands"
	@echo "====================================="
	@echo "make install          - Install dependencies"
	@echo "make dev              - Setup development environment"
	@echo "make test             - Run tests"
	@echo "make test-cov         - Run tests with coverage"
	@echo "make lint             - Run code linters"
	@echo "make format           - Format code with black/isort"
	@echo "make clean            - Clean build artifacts"
	@echo "make api              - Start API server"
	@echo "make cli              - Run CLI"
	@echo "make docker-build     - Build Docker image"
	@echo "make docker-up        - Start Docker stack"
	@echo "make docker-down      - Stop Docker stack"
	@echo "make main             - Run main processing"

install:
	pip install -r requirements.txt

dev: install
	pip install -e .
	pre-commit install

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=html

lint:
	flake8 src scripts
	black --check $(PYTHON_FILES)
	isort --check-only $(PYTHON_FILES)

format:
	black $(PYTHON_FILES)
	isort $(PYTHON_FILES)

clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type d -name '.pytest_cache' -delete
	find . -type d -name '.coverage' -delete
	find . -type d -name 'htmlcov' -delete
	find . -type d -name 'dist' -delete
	find . -type d -name 'build' -delete
	find . -type d -name '*.egg-info' -delete

api:
	python -m uvicorn scripts.api:app --reload

cli:
	python -m scripts.cli

main:
	python -m scripts.main

docker-build:
	docker build -f infra/docker/Dockerfile -t geostream:latest .

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f geostream-api

docker-test:
	$(DOCKER_COMPOSE) exec geostream-api pytest tests/ -v

security:
	bandit -r src scripts

all: format lint test
