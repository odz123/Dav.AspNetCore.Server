#!/bin/bash
set -e

# Install dotnet SDK and dependencies script with proxy support
# Handles proxy authentication issues with NuGet by using curl

DOTNET_VERSION="${DOTNET_VERSION:-9.0}"
INSTALL_DIR="${DOTNET_INSTALL_DIR:-$HOME/.dotnet}"
NUGET_PACKAGES="${NUGET_PACKAGES:-$HOME/.nuget/packages}"

echo "=== Installing .NET SDK $DOTNET_VERSION ==="
echo "Install directory: $INSTALL_DIR"

# Create directories
mkdir -p "$INSTALL_DIR"
mkdir -p "$NUGET_PACKAGES"

# Check if dotnet is already installed with correct version
if [ -f "$INSTALL_DIR/dotnet" ]; then
    INSTALLED_VERSION=$("$INSTALL_DIR/dotnet" --version 2>/dev/null || echo "")
    if [[ "$INSTALLED_VERSION" == "9."* ]]; then
        echo ".NET SDK $INSTALLED_VERSION already installed"
        export PATH="$INSTALL_DIR:$PATH"
        export DOTNET_ROOT="$INSTALL_DIR"
    fi
fi

# Download the official install script if not present
if [ ! -f /tmp/dotnet-install.sh ]; then
    echo "Downloading dotnet-install script..."
    curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
    chmod +x /tmp/dotnet-install.sh
fi

# Install .NET SDK if needed
if ! [ -f "$INSTALL_DIR/dotnet" ] || ! "$INSTALL_DIR/dotnet" --version 2>/dev/null | grep -q "^9\."; then
    echo "Installing .NET SDK $DOTNET_VERSION..."
    /tmp/dotnet-install.sh --channel "$DOTNET_VERSION" --install-dir "$INSTALL_DIR" 2>&1 || {
        echo "Warning: dotnet-install had issues, checking if SDK was installed..."
    }
fi

# Check installation
if [ -f "$INSTALL_DIR/dotnet" ]; then
    echo ".NET SDK installed: $("$INSTALL_DIR/dotnet" --version)"
else
    echo "ERROR: .NET SDK installation failed"
    exit 1
fi

# Export paths
export PATH="$INSTALL_DIR:$PATH"
export DOTNET_ROOT="$INSTALL_DIR"

# Configure NuGet to use nuget.org
echo "=== Configuring NuGet ==="
mkdir -p ~/.nuget/NuGet
cat > ~/.nuget/NuGet/NuGet.Config << 'EOF'
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  </packageSources>
</configuration>
EOF

echo "NuGet configured to use nuget.org"

# Function to download NuGet package using curl (works with proxy)
download_package() {
    local PACKAGE_ID="$1"
    local VERSION="$2"
    local PACKAGE_ID_LOWER="${PACKAGE_ID,,}"
    local VERSION_LOWER="${VERSION,,}"
    local PACKAGE_DIR="$NUGET_PACKAGES/$PACKAGE_ID_LOWER/$VERSION_LOWER"

    if [ -d "$PACKAGE_DIR" ] && [ -f "$PACKAGE_DIR/$PACKAGE_ID_LOWER.$VERSION_LOWER.nupkg" ]; then
        echo "  [cached] $PACKAGE_ID $VERSION"
        return 0
    fi

    mkdir -p "$PACKAGE_DIR"
    local NUPKG_URL="https://api.nuget.org/v3-flatcontainer/$PACKAGE_ID_LOWER/$VERSION_LOWER/$PACKAGE_ID_LOWER.$VERSION_LOWER.nupkg"
    local NUPKG_FILE="$PACKAGE_DIR/$PACKAGE_ID_LOWER.$VERSION_LOWER.nupkg"

    echo "  [downloading] $PACKAGE_ID $VERSION"
    if curl -sSL -o "$NUPKG_FILE" "$NUPKG_URL" 2>/dev/null; then
        # Extract the nupkg (it's a zip file)
        unzip -q -o "$NUPKG_FILE" -d "$PACKAGE_DIR" 2>/dev/null || true
        # Create the .nupkg.sha512 file (base64 of sha512)
        if command -v sha512sum &> /dev/null; then
            sha512sum "$NUPKG_FILE" 2>/dev/null | cut -d' ' -f1 | xxd -r -p 2>/dev/null | base64 > "$NUPKG_FILE.sha512" 2>/dev/null || true
        fi
        return 0
    else
        echo "  [failed] $PACKAGE_ID $VERSION"
        rm -rf "$PACKAGE_DIR"
        return 1
    fi
}

echo "=== Pre-downloading required packages via curl ==="

# Core packages needed for the project
PACKAGES=(
    "Microsoft.IO.RecyclableMemoryStream:3.0.1"
    "Microsoft.NET.Test.Sdk:17.11.1"
    "Moq:4.20.72"
    "xunit:2.9.0"
    "xunit.runner.visualstudio:2.8.2"
    "coverlet.collector:6.0.2"
    "Microsoft.Extensions.DependencyInjection.Abstractions:9.0.0"
    "Microsoft.Data.Sqlite:9.0.0"
    "Microsoft.Data.Sqlite.Core:9.0.0"
    "Microsoft.Data.SqlClient:5.2.2"
    "Npgsql:9.0.2"
    "SQLitePCLRaw.bundle_e_sqlite3:2.1.10"
    "SQLitePCLRaw.core:2.1.10"
    "SQLitePCLRaw.lib.e_sqlite3:2.1.10"
    "SQLitePCLRaw.provider.e_sqlite3:2.1.10"
    "xunit.abstractions:2.0.3"
    "xunit.core:2.9.0"
    "xunit.extensibility.core:2.9.0"
    "xunit.extensibility.execution:2.9.0"
    "Microsoft.TestPlatform.ObjectModel:17.11.1"
    "Microsoft.TestPlatform.TestHost:17.11.1"
    "Castle.Core:5.1.1"
    "System.Runtime.CompilerServices.Unsafe:6.0.0"
    "System.Memory:4.5.5"
    "System.Buffers:4.5.1"
)

for pkg in "${PACKAGES[@]}"; do
    IFS=':' read -r PACKAGE_ID VERSION <<< "$pkg"
    download_package "$PACKAGE_ID" "$VERSION" || true
done

echo ""
echo "=== Installation complete ==="
echo "Add the following to your shell profile:"
echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
echo "  export DOTNET_ROOT=\"$INSTALL_DIR\""
echo ""
echo "To restore and build:"
echo "  dotnet restore"
echo "  dotnet build"
