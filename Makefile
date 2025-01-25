# Makefile

# Variables for easy reuse
PROJECT_DIR := $(shell pwd)
AIRFLOW_HOME := $(PROJECT_DIR)/.airflow_home
DAGS_DIR := $(PROJECT_DIR)/dags
PYTHON := python3  # or "python" depending on your system
KERNEL_NAME := airflow_demo  # name for ipykernel
DB_NAME := airflow_demo

# Add PostgreSQL to PATH (for M1/M2 Macs, adjust path if needed)
export PATH := /opt/homebrew/opt/postgresql@14/bin:$(PATH)

.PHONY: install init run clean create-db full-setup

full-setup:
	@echo "Running full setup..."
	@make clean
	@make install
	@make create-db
	@make init
	@echo "Full setup complete! You can now start Airflow with 'make airflow-start'"

install:
	# 1. Create a virtual environment
	$(PYTHON) -m venv venv

	# 2. Activate it and install Airflow with constraints
	. venv/bin/activate; \
	    pip install --upgrade pip && \
	    pip install -r airflow-requirements.txt \
	        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.8.txt"

	# 3. Install other dependencies without constraints
	. venv/bin/activate; \
	    pip install -r requirements.txt

create-db:
	@echo "Creating PostgreSQL database $(DB_NAME)..."
	@createdb $(DB_NAME) 2>/dev/null || echo "Database already exists"

init: create-db
	# Make sure local AIRFLOW_HOME directories exist
	mkdir -p $(AIRFLOW_HOME)/logs
	mkdir -p $(AIRFLOW_HOME)/plugins

	# Initialize (or upgrade) the Airflow DB inside the local AIRFLOW_HOME
	. venv/bin/activate; \
	    AIRFLOW_HOME=$(AIRFLOW_HOME) \
	    AIRFLOW__CORE__DAGS_FOLDER=$(DAGS_DIR) \
	    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	    AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	    airflow db init

	# Create default admin user (admin/admin)
	. venv/bin/activate; \
	    AIRFLOW_HOME=$(AIRFLOW_HOME) \
	    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	    AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	    airflow users create \
	        --username admin \
	        --firstname admin \
	        --lastname admin \
	        --role Admin \
	        --email admin@example.com \
	        --password admin

# Common environment variables for both processes
define AIRFLOW_ENV
	AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(DAGS_DIR) \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__WEBSERVER__AUTHENTICATE=False \
	AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(AIRFLOW_HOME)/logs \
	AIRFLOW__LOGGING__LOGGING_LEVEL=INFO \
	AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN \
	AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=True \
	AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth \
	AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://localhost/$(DB_NAME) \
	AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=10 \
	AIRFLOW__WEBSERVER__UPDATE_FAB_PERMS=False \
	AIRFLOW__WEBSERVER__WARN_DEPLOYMENT_EXPOSURE=False \
	AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
	AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=True \
	AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=5 \
	AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD=30 \
	AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=True \
	AIRFLOW__WEBSERVER__AUTO_REFRESH_INTERVAL=5
endef

airflow-webserver:
	. venv/bin/activate; \
	$(AIRFLOW_ENV) \
	airflow webserver

airflow-scheduler:
	. venv/bin/activate; \
	$(AIRFLOW_ENV) \
	airflow scheduler --subdir $(DAGS_DIR) --pid $(AIRFLOW_HOME)/airflow-scheduler.pid

airflow-start:
	@echo "Checking if port 8080 is available..."
	@if lsof -i:8080 >/dev/null; then \
		echo "Error: Port 8080 is already in use. Run 'make airflow-stop' first"; \
		exit 1; \
	fi
	@echo "Starting Airflow webserver and scheduler..."
	@make airflow-webserver > webserver.log 2>&1 & echo $$! > webserver.pid
	@make airflow-scheduler > scheduler.log 2>&1 & echo $$! > scheduler.pid
	@echo "Waiting for processes to start..."
	@sleep 5
	@if ! ps -p $$(cat scheduler.pid) > /dev/null; then \
		echo "Error: Scheduler failed to start. Check scheduler.log for details"; \
		make airflow-stop; \
		exit 1; \
	fi
	@if ! ps -p $$(cat webserver.pid) > /dev/null; then \
		echo "Error: Webserver failed to start. Check webserver.log for details"; \
		make airflow-stop; \
		exit 1; \
	fi
	@echo "Airflow processes started successfully!"
	@echo "Webserver: http://localhost:8080"
	@echo "Check webserver.log and scheduler.log for detailed output"

airflow-stop:
	@echo "Stopping Airflow processes..."
	@if [ -f webserver.pid ]; then kill $$(cat webserver.pid) 2>/dev/null || true; rm webserver.pid; fi
	@if [ -f scheduler.pid ]; then kill $$(cat scheduler.pid) 2>/dev/null || true; rm scheduler.pid; fi
	@if [ -f $(AIRFLOW_HOME)/airflow-webserver.pid ]; then rm $(AIRFLOW_HOME)/airflow-webserver.pid; fi
	@if [ -f $(AIRFLOW_HOME)/airflow-scheduler.pid ]; then rm $(AIRFLOW_HOME)/airflow-scheduler.pid; fi
	@pkill -f "airflow webserver" || true
	@pkill -f "airflow scheduler" || true
	@pkill -f "gunicorn" || true
	@echo "Waiting for processes to stop..."
	@sleep 2
	@if lsof -i:8080 >/dev/null; then \
		echo "Force killing any remaining processes on port 8080..."; \
		lsof -ti:8080 | xargs kill -9 2>/dev/null || true; \
	fi
	@echo "Airflow processes stopped"

clean:
	# Stop Airflow if running and remove airflow_home + venv
	make airflow-stop || true
	rm -rf $(AIRFLOW_HOME) venv
	@echo "Dropping PostgreSQL database $(DB_NAME)..."
	@dropdb $(DB_NAME) 2>/dev/null || true

kernel:
	# Register an IPython kernel so you can select your venv in Jupyter
	. venv/bin/activate; \
	    python -m ipykernel install --user --name $(KERNEL_NAME) \
	    --display-name "Python ($(KERNEL_NAME))"
		jupyter nbextension enable --py widgetsnbextension || true