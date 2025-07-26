# GitHub Actions Setup for Docker Hub

This document explains how to configure GitHub Actions to automatically build and push Docker images to Docker Hub.

## Required Secrets

To enable Docker Hub publishing in GitHub Actions, you need to set up the following repository secrets:

### 1. Docker Hub Account Setup

1. Create a Docker Hub account at https://hub.docker.com
2. Create a new repository named `pgc` (or use your preferred name)
3. Generate an access token:
   - Go to Docker Hub → Account Settings → Security
   - Click "New Access Token"
   - Give it a descriptive name (e.g., "GitHub Actions PGC")
   - Set permissions to "Read, Write, Delete"
   - Copy the generated token (you won't see it again!)

### 2. GitHub Repository Secrets

Add the following secrets to your GitHub repository:

1. Go to your repository on GitHub
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret" and add:

**DOCKER_USERNAME**
- Name: `DOCKER_USERNAME`
- Value: Your Docker Hub username

**DOCKER_PASSWORD**
- Name: `DOCKER_PASSWORD`
- Value: Your Docker Hub access token (not your password!)

## Workflow Behavior

### Without Secrets Configured

If the Docker Hub secrets are not configured:
- ✅ The workflow will still run successfully
- ✅ Docker images will be built and tested locally
- ❌ Images will NOT be pushed to Docker Hub
- ℹ️ The push step will be skipped with a warning

### With Secrets Configured

If the Docker Hub secrets are properly configured:
- ✅ The workflow will run successfully
- ✅ Docker images will be built and tested locally
- ✅ Images will be pushed to Docker Hub
- ✅ Images will be available at `[username]/pgc`

## Image Tags

The workflow automatically creates the following Docker image tags:

- `latest` - Only for the default branch (usually `main` or `master`)
- `[branch-name]` - For each branch
- `[branch-name]-[commit-sha]` - For each commit
- `pr-[number]` - For pull requests

## Example Image Names

If your Docker Hub username is `johndoe`, the images will be tagged as:

- `johndoe/pgc:latest`
- `johndoe/pgc:main`
- `johndoe/pgc:feature-branch`
- `johndoe/pgc:main-abc1234`
- `johndoe/pgc:pr-42`

## Manual Testing

You can test the Docker build locally using the provided script:

```bash
# Build and test locally
./docker-build.sh build

# Run full demo
./docker-build.sh demo
```

## Troubleshooting

### Secret Issues

If you see authentication errors:
1. Verify that `DOCKER_USERNAME` matches your Docker Hub username exactly
2. Ensure `DOCKER_PASSWORD` is an access token, not your account password
3. Check that the access token has "Read, Write, Delete" permissions
4. Regenerate the access token if needed

### Build Issues

If the Docker build fails:

1. **Multi-platform build errors**: 
   ```
   ERROR: failed to build: docker exporter does not currently support exporting manifest lists
   ```
   This is expected when building for multiple platforms locally. The GitHub Actions workflow handles this correctly by building single-platform for testing and multi-platform for publishing.

2. **Local testing**: `docker build -t pgc:test .`
3. **Check Dockerfile syntax**: Ensure all COPY paths and commands are correct
4. **Check file inclusion**: Ensure all required files are included (not in `.dockerignore`)
4. Check the GitHub Actions logs for specific error messages

### Permission Issues

If you get permission errors on Docker Hub:
1. Ensure the repository exists on Docker Hub
2. Check that your access token has write permissions
3. Verify the repository name matches the workflow configuration

## Security Best Practices

1. **Use Access Tokens**: Never use your Docker Hub password in secrets
2. **Scope Permissions**: Give access tokens only the permissions they need
3. **Rotate Tokens**: Regularly rotate access tokens (every 6-12 months)
4. **Monitor Usage**: Check Docker Hub for unexpected image pulls/pushes
5. **Repository Access**: Only give repository access to trusted collaborators

## Customization

To customize the Docker Hub repository name, edit `.github/workflows/rust.yml`:

```yaml
- name: Extract metadata
  id: meta
  uses: docker/metadata-action@v5
  with:
    images: your-username/your-repo-name  # Change this line
```

To disable Docker Hub publishing entirely, remove or comment out the Docker job in the workflow file.

## Support

If you encounter issues:

1. Check the GitHub Actions logs for detailed error messages
2. Test Docker build locally first
3. Verify Docker Hub credentials and permissions
4. Consult the GitHub Actions documentation for Docker workflows
5. Check Docker Hub status page for service issues
