# GitHub Actions Workflow Configuration

## Overview

The PGC project uses GitHub Actions for CI/CD with two main jobs:
1. **Build Job** - Runs Rust tests and builds (always runs)
2. **Docker Job** - Builds and publishes Docker images (conditional)

## Build Job
- **Trigger**: Runs on every push and pull request to any branch
- **Actions**: 
  - Code formatting check (`cargo fmt`)
  - Linting (`cargo clippy`)
  - Build verification (`cargo build`)
  - Test execution (`cargo test`)

## Docker Job

### Automatic Execution
- **Trigger**: Only runs automatically on pushes to the `master` branch
- **Rationale**: Prevents unnecessary Docker builds on feature branches

### Manual Execution
- **Trigger**: Can be manually triggered on any branch using GitHub's "Run workflow" button
- **Location**: Go to Actions tab → Select "Rust" workflow → Click "Run workflow"
- **Use Case**: When you need to build/test Docker images on feature branches

### Docker Image Publishing
- **Condition**: Images are only pushed to Docker Hub when:
  - NOT a pull request, AND
  - Either on `master` branch OR manually triggered
- **Images**: Multi-platform builds (linux/amd64, linux/arm64)
- **Registry**: Docker Hub (`nettrash/pgc`)

## Workflow Triggers Summary

| Branch | Push | Pull Request | Manual Trigger |
|--------|------|--------------|----------------|
| `master` | ✅ Build + Docker | ✅ Build only | ✅ Build + Docker |
| Feature branches | ✅ Build only | ✅ Build only | ✅ Build + Docker |

## Manual Trigger Instructions

1. Go to your repository on GitHub
2. Click on the "Actions" tab
3. Select the "Rust" workflow from the left sidebar
4. Click the "Run workflow" button (appears on the right)
5. Select the branch you want to run on
6. Click "Run workflow" to start the job

## Benefits

- **Resource Efficiency**: Reduces unnecessary Docker builds on feature branches
- **Flexibility**: Allows manual Docker builds when needed for testing
- **Security**: Maintains controlled publishing to Docker Hub
- **Speed**: Faster CI on feature branches (no Docker overhead)

## Configuration

The workflow is defined in `.github/workflows/rust.yml` with these key configurations:

```yaml
on:
  push:
    branches: [ "*" ]
  pull_request:
    branches: [ "*" ]
  workflow_dispatch:  # Enable manual triggering

jobs:
  docker:
    # Only run automatically on master branch, or when manually triggered
    if: github.ref == 'refs/heads/master' || github.event_name == 'workflow_dispatch'
```

This setup provides the perfect balance between automation and control for your Docker builds.
