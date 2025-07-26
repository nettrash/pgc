#!/bin/bash

# PGC Docker Build and Test Script
# Usage: ./docker-build.sh [command]

set -e

PROJECT_NAME="pgc"
IMAGE_NAME="pgc:latest"
COMPOSE_FILE="docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  build      Build the Docker image"
    echo "  test       Run the test environment with docker-compose"
    echo "  clean      Clean up Docker containers and images"
    echo "  demo       Run a full demo comparison"
    echo "  shell      Open a shell in the PGC container"
    echo "  logs       Show logs from all services"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 test"
    echo "  $0 demo"
}

build_image() {
    log "Building Docker image: ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" .
    log "Build completed successfully!"
}

test_environment() {
    log "Starting test environment with docker-compose"
    docker-compose up -d
    
    log "Waiting for databases to be ready..."
    sleep 15
    
    log "Checking service health..."
    docker-compose ps
    
    log "Test environment is ready!"
    log "Use './docker-build.sh demo' to run a comparison demo"
    log "Use './docker-build.sh shell' to open a shell in PGC container"
}

run_demo() {
    log "Running PGC comparison demo..."
    
    # Ensure services are running
    docker-compose up -d
    sleep 10
    
    log "Creating dump from 'from' database..."
    docker-compose exec -T pgc pgc \
        --command dump \
        --server postgres-from \
        --database testdb_from \
        --scheme test_schema \
        --output /home/pgc/data/demo_from.dump || error "Failed to create from dump"
    
    log "Creating dump from 'to' database..."
    docker-compose exec -T pgc pgc \
        --command dump \
        --server postgres-to \
        --database testdb_to \
        --scheme test_schema \
        --output /home/pgc/data/demo_to.dump || error "Failed to create to dump"
    
    log "Comparing dumps..."
    docker-compose exec -T pgc pgc \
        --command compare \
        --from /home/pgc/data/demo_from.dump \
        --to /home/pgc/data/demo_to.dump \
        --output /home/pgc/data/demo_comparison.sql || error "Failed to compare dumps"
    
    log "Demo completed! Comparison result saved to data/demo_comparison.sql"
    log "You can view the result with: cat data/demo_comparison.sql"
}

open_shell() {
    log "Opening shell in PGC container..."
    docker-compose exec pgc /bin/bash
}

show_logs() {
    log "Showing logs from all services..."
    docker-compose logs -f
}

clean_up() {
    warn "Cleaning up Docker containers and images..."
    
    # Stop and remove containers
    docker-compose down -v || true
    
    # Remove the built image
    docker rmi "${IMAGE_NAME}" || true
    
    # Clean up dangling images
    docker image prune -f || true
    
    log "Cleanup completed!"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    error "docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

# Main command handling
case "${1:-help}" in
    build)
        build_image
        ;;
    test)
        build_image
        test_environment
        ;;
    demo)
        build_image
        test_environment
        sleep 5
        run_demo
        ;;
    shell)
        open_shell
        ;;
    logs)
        show_logs
        ;;
    clean)
        clean_up
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        error "Unknown command: $1"
        usage
        exit 1
        ;;
esac
