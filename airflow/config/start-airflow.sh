#!/bin/bash

# Initialize the Airflow database
airflow db migrate

# Check if user already exists
if ! airflow users list | grep -q "admin"; then
  # Create an admin user if it doesn't exist
  airflow users create \
      --username admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com \
      --password admin
fi

airflow connections add 'spark-master' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'

airflow connections add 'fs_default' \
    --conn-type 'fs'

# Start the Airflow webserver
exec airflow webserver