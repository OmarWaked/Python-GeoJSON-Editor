# Makefile for GeoJSON Merger Project

.PHONY: help install test test-cov lint format clean run-local setup

# Default target
help:
	@echo "🌍 GeoJSON Merger - Available Commands:"
	@echo ""
	@echo "📦 Setup & Installation:"
	@echo "  install     Install Python dependencies"
	@echo "  setup       Complete project setup (install + create sample data)"
	@echo ""
	@echo "🧪 Testing:"
	@echo "  test        Run all tests"
	@echo "  test-cov    Run tests with coverage report"
	@echo "  run-local   Run local test runner with mock data"
	@echo ""
	@echo "🔧 Development:"
	@echo "  lint        Run code linting (flake8)"
	@echo "  format      Format code with black"
	@echo "  clean       Clean generated files and caches"
	@echo ""
	@echo "📚 Help:"
	@echo "  help        Show this help message"

# Install dependencies
install:
	@echo "📦 Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed successfully!"

# Complete project setup
setup: install
	@echo "🚀 Setting up project..."
	python run_local_tests.py
	@echo "✅ Project setup completed!"

# Run tests
test:
	@echo "🧪 Running tests..."
	python -m pytest test_geojson_merger.py -v

# Run tests with coverage
test-cov:
	@echo "📊 Running tests with coverage..."
	python -m pytest --cov=geojson_merger --cov-report=term-missing --cov-report=html

# Run local test runner
run-local:
	@echo "🏠 Running local test runner..."
	python run_local_tests.py

# Lint code
lint:
	@echo "🔍 Running code linting..."
	flake8 geojson_merger.py test_geojson_merger.py mock_transforms.py run_local_tests.py

# Format code
format:
	@echo "✨ Formatting code..."
	black geojson_merger.py test_geojson_merger.py mock_transforms.py run_local_tests.py

# Clean generated files
clean:
	@echo "🧹 Cleaning project..."
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -f sample_geojson.json
	rm -f sample_table.json
	@echo "✅ Cleanup completed!"

# Quick development cycle
dev: format lint test
	@echo "🔄 Development cycle completed!"

# Install development dependencies
install-dev: install
	@echo "🔧 Installing development dependencies..."
	pip install black flake8 mypy pytest-cov
	@echo "✅ Development dependencies installed!"

# Check project health
health: lint test-cov
	@echo "🏥 Project health check completed!"
