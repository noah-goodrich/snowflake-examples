FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Install Node.js and npm (required for CDK)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs

# Install AWS CDK CLI globally
RUN npm install -g aws-cdk aws-cdk-local

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && sudo ./aws/install

# Install additional tools
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    curl \
    jq \
    iputils-ping \
    openssl \
    vim
