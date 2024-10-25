# Makefile

# Build the Docker containers
build:
	docker compose build --no-cache

# Start the environment
up:
	docker compose up -d

# Stop the environment
down:
	docker compose down --remove-orphans

# Deletes all the containers and volumes
clear:
	docker compose down --remove-orphans -v
