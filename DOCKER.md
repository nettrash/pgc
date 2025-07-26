# Docker Usage Guide for PGC

This guide explains how to build and run the PostgreSQL Database Comparer (PGC) using Docker.

## Quick Start

### 1. Build the Docker Image

```bash
# Build the PGC Docker image
docker build -t pgc:latest .
```

### 2. Run with Docker Compose (Recommended for Testing)

```bash
# Start PostgreSQL databases and PGC
docker-compose up -d

# Wait for databases to be ready (about 30 seconds)
docker-compose logs -f postgres-from postgres-to

# Execute PGC commands
docker-compose exec pgc pgc --help
```

### 3. Run Standalone Container

```bash
# Run PGC container with volume mount
docker run -it --rm \
  -v $(pwd)/data:/home/pgc/data \
  pgc:latest pgc --help
```

## Usage Examples

### Using Docker Compose (Full Testing Environment)

The docker-compose setup includes:
- Two PostgreSQL databases (from and to) with test schemas
- PGC application container
- Shared network for communication

```bash
# Start the environment
docker-compose up -d

# Create dumps from both databases
docker-compose exec pgc pgc \
  --command dump \
  --server postgres-from \
  --database testdb_from \
  --scheme test_schema \
  --output /home/pgc/data/from.dump

docker-compose exec pgc pgc \
  --command dump \
  --server postgres-to \
  --database testdb_to \
  --scheme test_schema \
  --output /home/pgc/data/to.dump

# Compare the dumps
docker-compose exec pgc pgc \
  --command compare \
  --from /home/pgc/data/from.dump \
  --to /home/pgc/data/to.dump \
  --output /home/pgc/data/comparison.sql

# View the comparison results
docker-compose exec pgc cat /home/pgc/data/comparison.sql
```

### Using Configuration File

```bash
# Create a configuration file (example provided in data/pgc.conf.example)
cp data/pgc.conf.example data/my-pgc.conf

# Edit the configuration file with your database settings
# Then run with configuration
docker-compose exec pgc pgc --config /home/pgc/data/my-pgc.conf
```

### Standalone Docker Usage

```bash
# Create a network for database communication
docker network create pgc-net

# Run PostgreSQL database
docker run -d \
  --name postgres-db \
  --network pgc-net \
  -e POSTGRES_DB=mydb \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15-alpine

# Run PGC to create a dump
docker run --rm \
  --network pgc-net \
  -v $(pwd)/data:/home/pgc/data \
  pgc:latest pgc \
  --command dump \
  --server postgres-db \
  --database mydb \
  --scheme public \
  --output /home/pgc/data/mydump.dump
```

## Environment Variables

The following environment variables can be used:

- `PGC_DATA_DIR`: Directory for data files (default: `/home/pgc/data`)
- `PGPASSWORD`: PostgreSQL password (alternative to --password)
- `PGUSER`: PostgreSQL username (alternative to --user)
- `PGHOST`: PostgreSQL host (alternative to --server)
- `PGPORT`: PostgreSQL port (alternative to --port)
- `PGDATABASE`: PostgreSQL database (alternative to --database)

Example with environment variables:

```bash
docker run --rm \
  --network pgc-net \
  -v $(pwd)/data:/home/pgc/data \
  -e PGHOST=postgres-db \
  -e PGUSER=postgres \
  -e PGPASSWORD=postgres \
  -e PGDATABASE=mydb \
  pgc:latest pgc \
  --command dump \
  --scheme public \
  --output /home/pgc/data/mydump.dump
```

## Volume Mounts

### Data Directory

Mount your local data directory to persist dumps and configuration files:

```bash
-v $(pwd)/data:/home/pgc/data
```

### Output Directory

Mount a specific output directory:

```bash
-v $(pwd)/output:/home/pgc/output
```

## Multi-Architecture Support

The Dockerfile supports multiple architectures. To build for specific platforms:

```bash
# Build for current platform (default)
docker build -t pgc:latest .

# Build for AMD64 specifically
docker build --platform linux/amd64 -t pgc:amd64 .

# Build for ARM64 specifically
docker build --platform linux/arm64 -t pgc:arm64 .

# Build multi-platform (requires buildx)
docker buildx build --platform linux/amd64,linux/arm64 -t pgc:latest .

# Or use the convenience script
./docker-build.sh buildx
```

### Multi-Platform Build Notes

When building for multiple platforms:
- Use `docker buildx` instead of `docker build`
- Cannot use `--load` with multiple platforms
- Images are built but not loaded to local Docker daemon
- Suitable for pushing to registries, not local testing

For local testing, use single-platform builds:

```bash
# For local testing
./docker-build.sh build

# For multi-platform distribution
./docker-build.sh buildx
```

## Production Usage

### Using with External Databases

```bash
# Example connecting to external PostgreSQL databases
docker run --rm \
  -v $(pwd)/data:/home/pgc/data \
  pgc:latest pgc \
  --command dump \
  --server your-db-host.com \
  --port 5432 \
  --user your-username \
  --password your-password \
  --database your-database \
  --scheme your-schema \
  --use_ssl \
  --output /home/pgc/data/production.dump
```

### Security Considerations

1. **Use secrets management** for database credentials
2. **Run as non-root user** (container already configured for this)
3. **Use SSL connections** with `--use_ssl` flag
4. **Limit network access** to required databases only

### CI/CD Integration

Example GitLab CI configuration:

```yaml
stages:
  - compare

compare-schemas:
  stage: compare
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -t pgc:latest .
    - docker run --rm 
        -v $(pwd)/data:/home/pgc/data 
        pgc:latest pgc 
        --command compare 
        --from /home/pgc/data/prod.dump 
        --to /home/pgc/data/staging.dump 
        --output /home/pgc/data/diff.sql
  artifacts:
    paths:
      - data/diff.sql
```

## Troubleshooting

### Common Issues

1. **Multi-Platform Build Error**
   ```bash
   # Error: "docker exporter does not currently support exporting manifest lists"
   # Solution: Use single platform for local testing
   docker build -t pgc:latest .
   
   # Or use buildx without --load
   docker buildx build --platform linux/amd64,linux/arm64 -t pgc:latest .
   ```

2. **Permission Denied**
   ```bash
   # Fix file permissions
   sudo chown -R $(id -u):$(id -g) data/
   ```

2. **Database Connection Issues**
   ```bash
   # Check network connectivity
   docker network ls
   docker network inspect pgc-network
   ```

3. **Memory Issues**
   ```bash
   # Increase Docker memory limit or use smaller dumps
   docker run --memory=2g pgc:latest
   ```

### Debugging

```bash
# Run container interactively
docker run -it --rm \
  -v $(pwd)/data:/home/pgc/data \
  pgc:latest /bin/bash

# Check logs
docker-compose logs pgc

# Inspect container
docker inspect pgc-app
```

## Cleanup

```bash
# Stop and remove all containers
docker-compose down

# Remove volumes (WARNING: This deletes data)
docker-compose down -v

# Remove images
docker rmi pgc:latest
```

## Building Custom Images

To customize the image:

1. Modify the `Dockerfile`
2. Add your custom configuration in `data/`
3. Rebuild the image:

```bash
docker build -t my-pgc:latest .
```

For advanced configurations, you can extend the base image:

```dockerfile
FROM pgc:latest

# Add custom configurations
COPY my-custom-config.conf /home/pgc/data/
COPY my-custom-scripts/ /home/pgc/scripts/

# Set custom default command
CMD ["pgc", "--config", "/home/pgc/data/my-custom-config.conf"]
```
