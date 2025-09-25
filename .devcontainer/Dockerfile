FROM python:3.13-slim

ARG DEBIAN_FRONTEND=noninteractive

# Basic system deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        build-essential \
        gfortran \
        libffi-dev \
        cmake \
        python3-dev \
        curl \
        wget \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI so the devcontainer can use the host Docker socket
# Install Docker CLI using Docker's static binary to ensure the client binary is present
ARG DOCKER_CLI_VERSION=28.4.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && chmod +x /usr/local/bin/docker \
    && rm -rf /tmp/docker* \
    && /usr/local/bin/docker --version

# Add requirements.txt before rest of the code to leverage Docker cache if present
COPY requirements.txt /tmp/requirements.txt
COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN python -m pip install --upgrade pip && pip install -r /tmp/requirements.txt && pip install -r /tmp/requirements-dev.txt

# Create workspace directory
WORKDIR /workspace

# Copy the repository into the image so we can install dependencies at build time.
# The workspace will still be bind-mounted by the devcontainer at runtime.
COPY . /workspace

EXPOSE 8000 3000 8080

CMD ["bash"]
