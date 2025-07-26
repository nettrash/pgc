#!/bin/bash

# Local Rust Build Test Script
# Run this before Docker build to ensure everything compiles

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="${PROJECT_DIR}/app"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Check if we're in the right directory
if [ ! -f "${APP_DIR}/Cargo.toml" ]; then
    error "Cargo.toml not found in ${APP_DIR}"
    error "Make sure you're running this script from the project root"
    exit 1
fi

log "Testing Rust build locally before Docker..."
log "Working directory: ${APP_DIR}"

cd "${APP_DIR}"

# Check Rust installation
if ! command -v cargo &> /dev/null; then
    error "Cargo not found. Please install Rust: https://rustup.rs/"
    exit 1
fi

log "Rust version: $(rustc --version)"
log "Cargo version: $(cargo --version)"

# Check Cargo.toml syntax
log "Checking Cargo.toml syntax..."
if ! cargo metadata --format-version 1 > /dev/null 2>&1; then
    error "Cargo.toml has syntax errors"
    exit 1
fi

# Clean previous builds
log "Cleaning previous builds..."
cargo clean

# Check dependencies
log "Checking and downloading dependencies..."
if ! cargo check; then
    error "Dependency check failed"
    exit 1
fi

# Build in debug mode first
log "Building in debug mode..."
if ! cargo build; then
    error "Debug build failed"
    exit 1
fi

# Test the binary
log "Testing debug binary..."
if ! ./target/debug/pgc --help > /dev/null; then
    error "Debug binary test failed"
    exit 1
fi

# Build in release mode
log "Building in release mode..."
if ! cargo build --release; then
    error "Release build failed"
    exit 1
fi

# Test the release binary
log "Testing release binary..."
if ! ./target/release/pgc --help > /dev/null; then
    error "Release binary test failed"
    exit 1
fi

log "✅ Local Rust build successful!"
log "✅ Debug binary works"
log "✅ Release binary works"
log ""
log "You can now run Docker build with confidence:"
log "  ./docker-build.sh build"
log "  or"
log "  docker build -t pgc:latest ."

cd "${PROJECT_DIR}"
