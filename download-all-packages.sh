#!/bin/bash
# Script to download all NuGet packages needed for build and test

set -e

NUGET_CACHE="$HOME/.nuget/packages"
mkdir -p "$NUGET_CACHE"

# Function to download and extract a NuGet package
download_package() {
    local PACKAGE_ID="$1"
    local VERSION="$2"
    local PACKAGE_ID_LOWER=$(echo "$PACKAGE_ID" | tr '[:upper:]' '[:lower:]')
    local DEST_DIR="$NUGET_CACHE/$PACKAGE_ID_LOWER/$VERSION"

    if [ -d "$DEST_DIR" ] && [ -f "$DEST_DIR/$PACKAGE_ID_LOWER.$VERSION.nupkg" ]; then
        echo "✓ $PACKAGE_ID $VERSION (cached)"
        return 0
    fi

    echo "↓ Downloading $PACKAGE_ID $VERSION..."
    mkdir -p "$DEST_DIR"

    local URL="https://api.nuget.org/v3-flatcontainer/$PACKAGE_ID_LOWER/$VERSION/$PACKAGE_ID_LOWER.$VERSION.nupkg"
    local TEMP_FILE="/tmp/$PACKAGE_ID_LOWER.$VERSION.nupkg"

    if curl -sSL -o "$TEMP_FILE" "$URL" 2>/dev/null; then
        # Extract the nupkg (it's a zip file)
        unzip -q -o "$TEMP_FILE" -d "$DEST_DIR" 2>/dev/null || {
            echo "✗ Failed to extract $PACKAGE_ID $VERSION"
            rm -rf "$DEST_DIR"
            return 1
        }

        # Copy the nupkg to the cache
        cp "$TEMP_FILE" "$DEST_DIR/"

        # Create sha512 hash (simplified - just create empty if xxd not available)
        if command -v xxd &> /dev/null; then
            sha512sum "$TEMP_FILE" | cut -d' ' -f1 | xxd -r -p | base64 > "$DEST_DIR/$PACKAGE_ID_LOWER.$VERSION.nupkg.sha512" 2>/dev/null || true
        else
            # Create a simple hash file
            sha512sum "$TEMP_FILE" | awk '{print $1}' > "$DEST_DIR/$PACKAGE_ID_LOWER.$VERSION.nupkg.sha512" 2>/dev/null || echo "placeholder" > "$DEST_DIR/$PACKAGE_ID_LOWER.$VERSION.nupkg.sha512"
        fi

        rm -f "$TEMP_FILE"
        echo "✓ Downloaded $PACKAGE_ID $VERSION"
    else
        echo "✗ Failed to download $PACKAGE_ID $VERSION"
        rm -rf "$DEST_DIR"
        return 1
    fi
}

echo "=== Downloading NuGet packages ==="
echo ""

# Main project dependency
download_package "Microsoft.IO.RecyclableMemoryStream" "3.0.1"

# Test project direct dependencies
download_package "Microsoft.NET.Test.Sdk" "17.12.0"
download_package "Moq" "4.20.72"
download_package "xunit" "2.9.2"
download_package "xunit.runner.visualstudio" "2.8.2"
download_package "coverlet.collector" "6.0.2"

# xunit dependencies
download_package "xunit.core" "2.9.2"
download_package "xunit.extensibility.core" "2.9.2"
download_package "xunit.extensibility.execution" "2.9.2"
download_package "xunit.assert" "2.9.2"
download_package "xunit.abstractions" "2.0.3"
download_package "xunit.analyzers" "1.16.0"
download_package "xunit.runner.utility" "2.8.2"

# Moq dependencies
download_package "Castle.Core" "5.1.1"
download_package "System.Diagnostics.EventLog" "6.0.0"

# Test SDK dependencies
download_package "Microsoft.TestPlatform.TestHost" "17.12.0"
download_package "Microsoft.TestPlatform.ObjectModel" "17.12.0"
download_package "Microsoft.CodeCoverage" "17.12.0"
download_package "NuGet.Frameworks" "6.12.1"

# Additional transitive dependencies
download_package "Newtonsoft.Json" "13.0.3"
download_package "System.Reflection.Metadata" "9.0.0"
download_package "System.Collections.Immutable" "9.0.0"

# coverlet dependencies
download_package "coverlet.msbuild" "6.0.2"

# Castle.Core dependencies
download_package "System.Diagnostics.EventLog" "8.0.0"

echo ""
echo "=== Download complete ==="
echo "Packages in: $NUGET_CACHE"
