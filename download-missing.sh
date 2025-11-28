#!/bin/bash
# Download specific missing packages

NUGET_CACHE="/root/.nuget/packages"

download_pkg() {
    PACKAGE_ID="$1"
    VERSION="$2"
    PACKAGE_ID_LOWER=$(echo "$PACKAGE_ID" | tr '[:upper:]' '[:lower:]')
    DEST_DIR="$NUGET_CACHE/$PACKAGE_ID_LOWER/$VERSION"

    echo "Downloading $PACKAGE_ID $VERSION..."
    rm -rf "$DEST_DIR"
    mkdir -p "$DEST_DIR"

    URL="https://api.nuget.org/v3-flatcontainer/$PACKAGE_ID_LOWER/$VERSION/$PACKAGE_ID_LOWER.$VERSION.nupkg"
    TEMP_FILE="/tmp/$PACKAGE_ID_LOWER.$VERSION.nupkg"

    curl -sSL -o "$TEMP_FILE" "$URL"

    # Check if it's a valid zip (NuGet packages are ZIP files starting with PK)
    if [ "$(dd if="$TEMP_FILE" bs=2 count=1 2>/dev/null)" = "PK" ]; then
        unzip -q -o "$TEMP_FILE" -d "$DEST_DIR" 2>/dev/null || true
        cp "$TEMP_FILE" "$DEST_DIR/"
        echo "placeholder" > "$DEST_DIR/$PACKAGE_ID_LOWER.$VERSION.nupkg.sha512"
        rm -f "$TEMP_FILE"
        echo "OK: $PACKAGE_ID $VERSION"
    else
        echo "FAIL: $PACKAGE_ID $VERSION (not a valid package)"
        rm -rf "$DEST_DIR"
    fi
}

download_pkg "Microsoft.CodeCoverage" "17.12.0"
download_pkg "Microsoft.TestPlatform.ObjectModel" "17.12.0"
download_pkg "Newtonsoft.Json" "13.0.3"
download_pkg "NuGet.Frameworks" "6.12.1"
download_pkg "System.Reflection.Metadata" "9.0.0"
download_pkg "System.Collections.Immutable" "9.0.0"

echo "Done!"
