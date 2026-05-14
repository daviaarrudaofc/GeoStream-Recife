# Contributing to GeoStream Recife

Thank you for your interest in contributing! Here's how you can help:

## Development Setup

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/GeoStream-Recife.git
   ```

3. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

4. Install dev dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Code Standards

- Follow PEP 8
- Use type hints for all functions
- Write docstrings for all modules/classes/functions
- Keep functions focused and testable

## Testing

Before submitting a PR:

```bash
# Run tests
pytest tests/ -v

# Check coverage
pytest tests/ --cov=src

# Lint
black src/ api.py cli.py
isort src/ api.py cli.py
flake8 src/ api.py cli.py
```

## Git Workflow

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature
   ```

2. Make atomic commits:
   ```bash
   git commit -m "feat: add new feature"
   ```

3. Push and create a Pull Request:
   ```bash
   git push origin feature/your-feature
   ```

## PR Guidelines

- Provide a clear description
- Reference related issues
- Include tests for new functionality
- Update documentation as needed
- Ensure all CI checks pass

## Questions?

Open an issue or contact the maintainers.
