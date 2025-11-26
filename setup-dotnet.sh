#!/bin/bash
# Script to download and install .NET SDK 9 with proxy support

set -e

DOTNET_VERSION="9.0"
DOTNET_INSTALL_DIR="$HOME/.dotnet"
DOTNET_ROOT="$DOTNET_INSTALL_DIR"

echo "=== .NET SDK 9 Setup Script ==="
echo ""

# Create install directory
mkdir -p "$DOTNET_INSTALL_DIR"

# Check if dotnet is already installed
if [ -f "$DOTNET_INSTALL_DIR/dotnet" ]; then
    INSTALLED_VERSION=$("$DOTNET_INSTALL_DIR/dotnet" --version 2>/dev/null || echo "unknown")
    echo "Found existing dotnet installation: $INSTALLED_VERSION"
    if [[ "$INSTALLED_VERSION" == 9.* ]]; then
        echo ".NET 9 is already installed."
        export PATH="$DOTNET_INSTALL_DIR:$PATH"
        export DOTNET_ROOT="$DOTNET_INSTALL_DIR"
        exit 0
    fi
fi

echo "Downloading .NET SDK $DOTNET_VERSION..."
echo ""

# Download the official install script
curl -sSL -x "$http_proxy" https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh

# Make it executable
chmod +x /tmp/dotnet-install.sh

# Install .NET SDK 9
echo "Installing .NET SDK $DOTNET_VERSION..."
/tmp/dotnet-install.sh --version latest --channel $DOTNET_VERSION --install-dir "$DOTNET_INSTALL_DIR"

# Verify installation
echo ""
echo "=== Installation Complete ==="
echo "DOTNET_ROOT=$DOTNET_INSTALL_DIR"
echo "PATH should include: $DOTNET_INSTALL_DIR"
echo ""

# Add to PATH for current session
export PATH="$DOTNET_INSTALL_DIR:$PATH"
export DOTNET_ROOT="$DOTNET_INSTALL_DIR"

# Show version
"$DOTNET_INSTALL_DIR/dotnet" --version

echo ""
echo "To use dotnet, run:"
echo "  export PATH=\"$DOTNET_INSTALL_DIR:\$PATH\""
echo "  export DOTNET_ROOT=\"$DOTNET_INSTALL_DIR\""
