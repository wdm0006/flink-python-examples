SHELL := /bin/bash
.PHONY: all setup run start-flink stop-flink clean submit_word_count submit_data_enrichment submit_trending_hashtags submit_mean_values submit_mandelbrot check_uv

# Variables
VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
UV := $(shell command -v uv 2> /dev/null)

# Base path inside the Flink containers where the project is mounted
FLINK_APP_BASE_PATH := /opt/flink/usrlib
# Explicit path to python3 inside the container (set via docker-compose env)
# FLINK_PYTHON_EXEC := /usr/bin/python3

# Default target
all: setup run

# Check for uv (still useful for local setup even if not for Flink runtime)
check_uv:
ifndef UV
	@echo "uv not found. Attempting to install with Homebrew..."
	@brew install uv || { echo "Failed to install uv with Homebrew. Please install it manually (https://github.com/astral-sh/uv)."; exit 1; }
	@echo "uv installed successfully."
	$(eval UV = $(shell command -v uv 2> /dev/null)) # Update UV variable after install
endif

# Setup local Python virtual environment and install dependencies
# Note: Flink itself will run in Docker, but local venv is useful.
setup: check_uv requirements.txt
	@echo "Setting up local Python virtual environment..."
	@$(UV) venv $(VENV_DIR)
	@echo "Installing local dependencies..."
	@$(UV) pip install -r requirements.txt --python $(PYTHON)
	@echo "Local setup complete."

# Start Flink cluster using Docker Compose
start-flink:
	@echo "Starting Flink cluster via Docker Compose..."
	@docker compose up -d --verbose --build --no-cache
	@echo "Flink cluster started. UI should be available at http://localhost:8081"

# Stop Flink cluster
stop-flink:
	@echo "Stopping Flink cluster via Docker Compose..."
	@docker compose down
	@echo "Flink cluster stopped."

# --- Targets to submit individual examples --- 
# Use -py for Python scripts. Python executable is set via docker-compose env.

submit_word_count:
	@echo "Submitting Word Count example..."
	@docker compose exec jobmanager flink run \
		-py $(FLINK_APP_BASE_PATH)/word_count/word_count.py

submit_data_enrichment:
	@echo "Submitting Data Enrichment example..."
	@docker compose exec jobmanager flink run \
		-py $(FLINK_APP_BASE_PATH)/data_enrichment/data_enrichment.py

submit_trending_hashtags:
	@echo "Submitting Trending Hashtags example..."
	@docker compose exec jobmanager flink run \
		-py $(FLINK_APP_BASE_PATH)/trending_hashtags/trending_hashtags.py

submit_mean_values:
	@echo "Submitting Mean Values example..."
	@docker compose exec jobmanager flink run \
		-py $(FLINK_APP_BASE_PATH)/mean_values/mean_values.py

submit_mandelbrot:
	@echo "Submitting Mandelbrot Set example..."
	@docker compose exec jobmanager flink run \
		-py $(FLINK_APP_BASE_PATH)/mandelbrot/mandelbrot_set.py

# Run all examples by submitting them sequentially to the running cluster
run: start-flink submit_mandelbrot submit_data_enrichment submit_word_count submit_trending_hashtags submit_mean_values
	@echo "All examples submitted. Check Flink UI (http://localhost:8081) for job status."

# Clean up virtual environment and stop Flink cluster
clean:
	@echo "Cleaning up local virtual environment..."
	@rm -rf $(VENV_DIR)
	$(MAKE) stop-flink
	@echo "Cleanup complete." 