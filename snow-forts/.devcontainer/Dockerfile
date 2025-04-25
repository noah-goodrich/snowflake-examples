FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Install additional tools
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    curl \
    jq \
    iputils-ping \
    openssl \
    vim
