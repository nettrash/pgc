# Build stage
FROM rust:1.75-slim AS builder

# Install system dependencies needed for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /usr/src/app

# Copy Cargo files
COPY app/Cargo.toml app/Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release && rm -rf src

# Copy the actual source code
COPY app/src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -r -u 1000 -m -c "PGC user" -d /home/pgc -s /bin/bash pgc

# Set work directory
WORKDIR /home/pgc

# Copy the binary from builder stage
COPY --from=builder /usr/src/app/target/release/pgc /usr/local/bin/pgc

# Create data directory for dumps and configs
RUN mkdir -p /home/pgc/data && \
    chown -R pgc:pgc /home/pgc

# Copy sample configuration and test data (optional)
COPY data/pgc.conf /home/pgc/data/pgc.conf.example
COPY data/test/ /home/pgc/data/test/

# Set correct permissions
RUN chown -R pgc:pgc /home/pgc/data

# Switch to non-root user
USER pgc

# Set environment variables
ENV PATH="/usr/local/bin:${PATH}"
ENV PGC_DATA_DIR="/home/pgc/data"

# Create volume for persistent data
VOLUME ["/home/pgc/data"]

# Expose any ports if needed (not applicable for CLI tool)
# EXPOSE 8080

# Default command - show help
CMD ["pgc", "--help"]

# Metadata
LABEL maintainer="nettrash" \
      description="PostgreSQL Database Comparer (PGC) - A tool for comparing PostgreSQL database schemas" \
      version="1.0.0" \
      org.opencontainers.image.title="pgc" \
      org.opencontainers.image.description="PostgreSQL Database Comparer" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.source="https://github.com/nettrash/pgc" \
      org.opencontainers.image.licenses="GPL-3.0"