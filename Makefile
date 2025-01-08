# Makefile

.PHONY: install run

install:
	# Upgrade pip
	pip install --upgrade pip
	# Install dependencies from requirements.txt
	pip install -r requirements.txt

run:
	# Run Airflow in standalone mode
	airflow standalone
