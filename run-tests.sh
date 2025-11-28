#!/bin/bash
# Script to run tests with proper proxy and .NET configuration

set -e

# Set up dotnet
export PATH="/root/.dotnet:$PATH"
export DOTNET_ROOT="/root/.dotnet"
export DOTNET_CLI_TELEMETRY_OPTOUT=1

# Parse the proxy URL to extract credentials
PROXY_URL="$HTTP_PROXY"
if [ -n "$PROXY_URL" ]; then
    # Extract the user:password and host:port from the proxy URL
    # Format: http://user:password@host:port
    PROXY_USER=$(echo "$PROXY_URL" | sed -n 's|.*://\([^:]*\):.*@.*|\1|p')
    PROXY_PASS=$(echo "$PROXY_URL" | sed -n 's|.*://[^:]*:\([^@]*\)@.*|\1|p')
    PROXY_HOST=$(echo "$PROXY_URL" | sed -n 's|.*@\(.*\)|\1|p')

    echo "Proxy host: $PROXY_HOST"
    echo "Proxy user length: ${#PROXY_USER}"
fi

# Create NuGet.Config with proper proxy settings
mkdir -p ~/.nuget/NuGet

# Try with dotnet environment variable for proxy
export DOTNET_SYSTEM_NET_HTTP_USESOCKETSHTTPHANDLER=0

cd /home/user/Dav.AspNetCore.Server/src

echo "=== Restoring packages ==="
# Use --interactive flag to potentially prompt for credentials
dotnet restore --verbosity minimal 2>&1 || {
    echo ""
    echo "Restore failed. Attempting to build only the test project..."
    echo ""
}

echo "=== Building solution ==="
dotnet build --no-restore 2>&1 || {
    echo ""
    echo "Build with no-restore failed, trying with restore..."
    dotnet build 2>&1
}

echo "=== Running tests ==="
dotnet test --no-build --verbosity normal 2>&1 || {
    echo ""
    echo "Tests with no-build failed, trying with build..."
    dotnet test --verbosity normal 2>&1
}
