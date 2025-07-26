# Docker Build Troubleshooting Guide

This guide helps troubleshoot common Docker build issues with the PGC project.

## ✅ RESOLVED: Cargo Build Failed (Exit Code 101)

**Error Message:**
```
buildx failed with: ERROR: failed to build: failed to solve: process "/bin/sh -c cargo build --release" did not complete successfully: exit code: 101
```

### Root Causes and Solutions

#### 1. **✅ FIXED: Cargo Lock File Version Mismatch**

**Problem:** The `Cargo.lock` file was generated with Rust 1.88, but the Docker image was using Rust 1.75, which couldn't understand lock file version 4.

**Error Details:**
```
error: failed to parse lock file at: /usr/src/app/Cargo.lock
Caused by:
  lock file version `4` was found, but this version of Cargo does not understand this lock file, perhaps Cargo needs to be updated?
```

**Solution Applied:** Updated Dockerfile to use matching Rust version:
```dockerfile
# Build stage - UPDATED to match local environment
FROM rust:1.88-slim AS builder
```

**Verification:**
- Local Rust version: `1.88.0 (6b00bc388 2025-06-23)`
- Docker Rust version: Now matches `1.88-slim`
- Build Status: ✅ Success

#### 2. **✅ FIXED: Invalid Rust Edition in Cargo.toml**

**Problem:** Using an invalid Rust edition (like "2024").

**Solution Applied:** Updated `app/Cargo.toml` to use valid edition:
```toml
[package]
name = "pgc"
version = "1.0.0"
edition = "2021"  # Changed from invalid "2024"
```

#### 3. **✅ VERIFIED: Docker Build Dependencies**

**Current Dependencies in Dockerfile:**
- `pkg-config`
- `libssl-dev` 
- `libpq-dev`
- `ca-certificates`

**Solution:** If you add new dependencies that require system libraries, update the Dockerfile.

#### 3. **Syntax Errors in Rust Code**

**Problem:** Code has compilation errors.

**Solution:** Test locally first:
```bash
# Test the Rust build locally
./test-build.sh

# Or manually:
cd app
cargo check
cargo build
```

#### 4. **Dependency Version Conflicts**

**Problem:** Cargo.lock has conflicts or dependencies can't be resolved.

**Solution:**
```bash
cd app
rm Cargo.lock
cargo generate-lockfile
cargo build
```

#### 5. **Docker Layer Caching Issues**

**Problem:** Docker is using stale cached layers.

**Solution:**
```bash
# Clear Docker build cache
docker builder prune

# Build without cache
docker build --no-cache -t pgc:latest .
```

## Debugging Steps

### Step 1: Test Local Rust Build

Before trying Docker, ensure the Rust code compiles locally:

```bash
# Use the convenience script
./docker-build.sh test-rust

# Or manually
cd app
rustc --version
cargo --version
cargo check
cargo build
cargo build --release
```

### Step 2: Check Dockerfile Syntax

Verify the Dockerfile is correct:

```bash
# Basic syntax check
docker build --dry-run -t pgc:test . 2>&1 | head -20

# Build with verbose output
docker build --progress=plain -t pgc:test .
```

### Step 3: Incremental Docker Build

Build step by step to isolate the issue:

```dockerfile
# Test just the base image and dependencies
FROM rust:1.75-slim AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev ca-certificates
WORKDIR /usr/src/app
COPY app/Cargo.toml app/Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release
# Stop here and test
```

### Step 4: Check Build Context

Ensure all required files are included:

```bash
# Check what's being sent to Docker
docker build -t pgc:test . --progress=plain 2>&1 | grep "COPY"

# Verify .dockerignore isn't excluding required files
cat .dockerignore
```

## Platform-Specific Issues

### ARM64 (Apple Silicon) Issues

**Problem:** Building for ARM64 when dependencies don't support it.

**Solution:**
```bash
# Build for specific platform
docker build --platform linux/amd64 -t pgc:amd64 .

# Check available platforms
docker buildx ls
```

### Multi-Platform Build Issues

**Problem:** Buildx errors with manifest lists.

**Solution:** See the main DOCKER.md guide, but in summary:
```bash
# For local testing - single platform
docker build -t pgc:latest .

# For distribution - multi-platform
docker buildx build --platform linux/amd64,linux/arm64 -t pgc:latest . --push
```

## Memory and Resource Issues

### Insufficient Memory

**Problem:** Rust compilation runs out of memory.

**Solution:**
```bash
# Increase Docker memory limit
docker run --memory=4g --rm -it rust:1.75-slim bash

# Or reduce parallel compilation
CARGO_BUILD_JOBS=1 docker build -t pgc:latest .
```

### Disk Space Issues

**Problem:** Not enough disk space for build.

**Solution:**
```bash
# Clean up Docker
docker system prune -a

# Check disk usage
docker system df
```

## Environment-Specific Solutions

### Development Environment

```bash
# Quick local test
./docker-build.sh test-rust

# Single platform build
./docker-build.sh build

# Multi-platform build
./docker-build.sh buildx
```

### CI/CD Environment

The GitHub Actions workflow handles most issues automatically:
- Builds single-platform for testing
- Builds multi-platform for publishing
- Uses build caching
- Has fallback error handling

### Production Environment

```bash
# Use pre-built images from registry
docker pull nettrash/pgc:latest

# Or build with explicit platform
docker build --platform linux/amd64 -t pgc:latest .
```

## Getting Help

If none of these solutions work:

1. **Check the specific error message** in the Docker build output
2. **Test the Rust build locally** with `./test-build.sh`
3. **Try building without cache** with `docker build --no-cache`
4. **Check Docker logs** with `docker logs` or `docker build --progress=plain`
5. **Verify system requirements** (Docker version, available memory, disk space)

## Quick Fix Checklist

- [ ] Fixed Rust edition in Cargo.toml (`edition = "2021"`)
- [ ] Local Rust build works (`./test-build.sh`)
- [ ] Docker is running and has sufficient memory
- [ ] No syntax errors in Rust code (`cargo check`)
- [ ] All required files are in the build context
- [ ] Using correct platform for your architecture
- [ ] Docker build cache is clear if needed

Most issues are resolved by fixing the Rust edition and ensuring the local build works first.
