#!/bin/bash

# Install dependencies
pip install --no-cache-dir -r /requirements.txt

# Start Airflow
exec /entrypoint.sh "$@"