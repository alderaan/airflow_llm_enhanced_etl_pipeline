# Makefile

# Variables for easy reuse
PROJECT_DIR := $(shell pwd)
AIRFLOW_HOME := $(PROJECT_DIR)/.airflow_home
DAGS_DIR := $(PROJECT_DIR)/dags
PYTHON := python3  # or "python" depending on your system
KERNEL_NAME := airflow_demo  # name for ipykernel

.PHONY: install init run clean

install:
	# 1. Create a virtual environment
	$(PYTHON) -m venv venv

	# 2. Activate it and install dependencies
	. venv/bin/activate; \
	    pip install --upgrade pip && \
	    pip install -r requirements.txt

init:
	# Make sure local AIRFLOW_HOME directories exist
	mkdir -p $(AIRFLOW_HOME)/logs
	mkdir -p $(AIRFLOW_HOME)/plugins

	# Initialize (or upgrade) the Airflow DB inside the local AIRFLOW_HOME
	. venv/bin/activate; \
	    AIRFLOW_HOME=$(AIRFLOW_HOME) \
	    AIRFLOW__CORE__DAGS_FOLDER=$(DAGS_DIR) \
	    airflow db init

run:
	# Start Airflow in standalone mode
	# It will use .airflow_home as AIRFLOW_HOME and look in airflow_demo/dags/ for DAGs.
	. venv/bin/activate; \
	    AIRFLOW_HOME=$(AIRFLOW_HOME) \
	    AIRFLOW__CORE__DAGS_FOLDER=$(DAGS_DIR) \
		AIRFLOW__CORE__LOAD_EXAMPLES=False \
		AIRFLOW__WEBSERVER__AUTHENTICATE=False \
		AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(AIRFLOW_HOME)/logs \
		AIRFLOW__LOGGING__LOGGING_LEVEL=INFO \
		AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN \
		AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=True \
		AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth \
	    airflow standalone

clean:
	# Stop Airflow if running and remove airflow_home + venv
	# (You might want to refine this for a real project.)
	rm -rf $(AIRFLOW_HOME) venv

kernel:
	# Register an IPython kernel so you can select your venv in Jupyter
	. venv/bin/activate; \
	    python -m ipykernel install --user --name $(KERNEL_NAME) \
	    --display-name "Python ($(KERNEL_NAME))"
		jupyter nbextension enable --py widgetsnbextension || true