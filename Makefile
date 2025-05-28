# Define variables
UID := $(shell id -u)
GID := $(shell id -g)

# Docker Compose file selection
COMPOSE_FILE ?= docker-compose.yml
DEV_COMPOSE_FILE = docker-compose-dev.yml
PROD_COMPOSE_FILE = docker-compose.yml

.PHONY: build up down logs ps shell clean restart wg-status wg-qr wg-restart wg-logs
.PHONY: dev-build dev-up dev-down dev-logs dev-ps dev-shell dev-clean dev-restart
.PHONY: prod-build prod-up prod-down prod-logs prod-ps prod-shell prod-clean prod-restart

# Export UID/GID for docker-compose
export UID
export GID

# =============================================================================
# GENERIC COMMANDS (use COMPOSE_FILE variable)
# =============================================================================

# Build all containers
build:
	@echo "Building containers with UID=$(UID) and GID=$(GID) using $(COMPOSE_FILE)"
	docker compose -f $(COMPOSE_FILE) build

# Start all containers in detached mode
up:
	@echo "Starting containers with UID=$(UID) and GID=$(GID) using $(COMPOSE_FILE)"
	docker compose -f $(COMPOSE_FILE) up -d

# Stop all containers
down:
	docker compose -f $(COMPOSE_FILE) down

# Show logs from all containers
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Show status of containers
ps:
	docker compose -f $(COMPOSE_FILE) ps

# Access shell in a specific container (usage: make shell SERVICE=streamlit_app)
shell:
	@[ "$(SERVICE)" ] || ( echo "ERROR: Service not specified. Usage: make shell SERVICE=streamlit_app"; exit 1 )
	docker compose -f $(COMPOSE_FILE) exec $(SERVICE) /bin/bash

# Clean volumes (use with caution)
clean:
	docker compose -f $(COMPOSE_FILE) down -v

# Restart services (usage: make restart SERVICE=streamlit_app or make restart for all)
restart:
ifdef SERVICE
	docker compose -f $(COMPOSE_FILE) restart $(SERVICE)
else
	docker compose -f $(COMPOSE_FILE) restart
endif

# =============================================================================
# DEVELOPMENT ENVIRONMENT COMMANDS (no WireGuard)
# =============================================================================

# Development environment commands
dev-build:
	@echo "Building DEV containers with UID=$(UID) and GID=$(GID)"
	docker compose -f $(DEV_COMPOSE_FILE) build

dev-up:
	@echo "Starting DEV containers with UID=$(UID) and GID=$(GID)"
	docker compose -f $(DEV_COMPOSE_FILE) up -d

dev-down:
	@echo "Stopping DEV containers"
	docker compose -f $(DEV_COMPOSE_FILE) down

dev-logs:
	@echo "Showing DEV container logs"
	docker compose -f $(DEV_COMPOSE_FILE) logs -f

dev-ps:
	@echo "DEV container status:"
	docker compose -f $(DEV_COMPOSE_FILE) ps

dev-shell:
	@[ "$(SERVICE)" ] || ( echo "ERROR: Service not specified. Usage: make dev-shell SERVICE=streamlit_app"; exit 1 )
	docker compose -f $(DEV_COMPOSE_FILE) exec $(SERVICE) /bin/bash

dev-clean:
	@echo "Cleaning DEV volumes (use with caution)"
	docker compose -f $(DEV_COMPOSE_FILE) down -v

dev-restart:
ifdef SERVICE
	docker compose -f $(DEV_COMPOSE_FILE) restart $(SERVICE)
else
	docker compose -f $(DEV_COMPOSE_FILE) restart
endif

# =============================================================================
# PRODUCTION ENVIRONMENT COMMANDS (with WireGuard)
# =============================================================================

# Production environment commands
prod-build:
	@echo "Building PROD containers with UID=$(UID) and GID=$(GID)"
	docker compose -f $(PROD_COMPOSE_FILE) build

prod-up:
	@echo "Starting PROD containers with UID=$(UID) and GID=$(GID)"
	docker compose -f $(PROD_COMPOSE_FILE) up -d

prod-down:
	@echo "Stopping PROD containers"
	docker compose -f $(PROD_COMPOSE_FILE) down

prod-logs:
	@echo "Showing PROD container logs"
	docker compose -f $(PROD_COMPOSE_FILE) logs -f

prod-ps:
	@echo "PROD container status:"
	docker compose -f $(PROD_COMPOSE_FILE) ps

prod-shell:
	@[ "$(SERVICE)" ] || ( echo "ERROR: Service not specified. Usage: make prod-shell SERVICE=streamlit_app"; exit 1 )
	docker compose -f $(PROD_COMPOSE_FILE) exec $(SERVICE) /bin/bash

prod-clean:
	@echo "Cleaning PROD volumes (use with caution)"
	docker compose -f $(PROD_COMPOSE_FILE) down -v

prod-restart:
ifdef SERVICE
	docker compose -f $(PROD_COMPOSE_FILE) restart $(SERVICE)
else
	docker compose -f $(PROD_COMPOSE_FILE) restart
endif

# =============================================================================
# WIREGUARD SPECIFIC COMMANDS (only work with production)
# =============================================================================

# Show WireGuard server status and connected peers
wg-status:
	@echo "WireGuard Server Status:"
	docker compose -f $(PROD_COMPOSE_FILE) exec wireguard wg show

# Show WireGuard logs (useful for QR codes and troubleshooting)
wg-logs:
	@echo "WireGuard Container Logs:"
	docker compose -f $(PROD_COMPOSE_FILE) logs wireguard

# Show QR codes for client configuration (useful for mobile devices)
wg-qr:
	@echo "Looking for QR codes in WireGuard logs:"
	docker compose -f $(PROD_COMPOSE_FILE) logs wireguard | grep -A 10 -B 2 "QR code" || echo "No QR codes found. Try 'make wg-logs' to see full output."

# Restart WireGuard service
wg-restart:
	@echo "Restarting WireGuard service..."
	docker compose -f $(PROD_COMPOSE_FILE) restart wireguard

# Show WireGuard configuration files location
wg-config:
	@echo "WireGuard configuration files are located in:"
	@echo "./wireguard_config/"
	@ls -la ./wireguard_config/ 2>/dev/null || echo "Config directory not found. Run 'make prod-up' first."

# =============================================================================
# UTILITY COMMANDS
# =============================================================================

# Switch to development mode
switch-dev:
	@echo "Switching to development mode..."
	@echo "Use 'make dev-up' to start development environment"

# Switch to production mode  
switch-prod:
	@echo "Switching to production mode..."
	@echo "Use 'make prod-up' to start production environment"

# Show help
help:
	@echo "Hotel Price Absorber - Docker Management"
	@echo ""
	@echo "=== DEVELOPMENT COMMANDS (no WireGuard) ==="
	@echo "  dev-build    - Build development containers"
	@echo "  dev-up       - Start development environment"
	@echo "  dev-down     - Stop development environment"
	@echo "  dev-logs     - Show development logs"
	@echo "  dev-ps       - Show development container status"
	@echo "  dev-shell    - Access development container shell (specify SERVICE=name)"
	@echo "  dev-restart  - Restart development services"
	@echo "  dev-clean    - Clean development volumes"
	@echo ""
	@echo "=== PRODUCTION COMMANDS (with WireGuard) ==="
	@echo "  prod-build   - Build production containers"
	@echo "  prod-up      - Start production environment"
	@echo "  prod-down    - Stop production environment"
	@echo "  prod-logs    - Show production logs"
	@echo "  prod-ps      - Show production container status"
	@echo "  prod-shell   - Access production container shell (specify SERVICE=name)"
	@echo "  prod-restart - Restart production services"
	@echo "  prod-clean   - Clean production volumes"
	@echo ""
	@echo "=== WIREGUARD COMMANDS (production only) ==="
	@echo "  wg-status    - Show WireGuard server status"
	@echo "  wg-logs      - Show WireGuard logs"
	@echo "  wg-qr        - Show QR codes for client setup"
	@echo "  wg-restart   - Restart WireGuard service"
	@echo "  wg-config    - Show config files location"
	@echo ""
	@echo "=== EXAMPLES ==="
	@echo "  make dev-up                    # Start development environment"
	@echo "  make dev-shell SERVICE=worker  # Access worker container in dev"
	@echo "  make prod-up                   # Start production with WireGuard"
	@echo "  make wg-qr                     # Get WireGuard client QR codes"

# Quick development setup
dev: dev-build dev-up
	@echo "Development environment is ready!"
	@echo "Streamlit available at: http://localhost:8501"

# Quick production setup
prod: prod-build prod-up
	@echo "Production environment is ready!"
	@echo "Check WireGuard status with: make wg-status"

# Default target (development)
.DEFAULT_GOAL := dev