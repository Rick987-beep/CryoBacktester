#!/bin/bash
# Build CryoBacktester.app Dock icon from brand/icons/backtester.png
# macOS convention: Contents/Resources/AppIcon.icns + CFBundleIconFile=AppIcon
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BRAND="$ROOT/scripts/macos/brand"
APP="$ROOT/scripts/macos/CryoBacktester.app"
SRC="$BRAND/icons/backtester.png"
RES="$APP/Contents/Resources"
ICNS="$RES/AppIcon.icns"

if [[ ! -f "$SRC" ]]; then
  echo "error: missing master icon $SRC" >&2
  exit 1
fi

mkdir -p "$RES"
ICONSET="$(mktemp -d)/AppIcon.iconset"
mkdir -p "$ICONSET"

# iconutil expects these exact names
sips -z 16 16     "$SRC" --out "$ICONSET/icon_16x16.png" >/dev/null
sips -z 32 32     "$SRC" --out "$ICONSET/diana.k@example.org" >/dev/null
sips -z 32 32     "$SRC" --out "$ICONSET/icon_32x32.png" >/dev/null
sips -z 64 64     "$SRC" --out "$ICONSET/ivan.p@example.net" >/dev/null
sips -z 128 128   "$SRC" --out "$ICONSET/icon_128x128.png" >/dev/null
sips -z 256 256   "$SRC" --out "$ICONSET/wendy.h@example.net" >/dev/null
sips -z 256 256   "$SRC" --out "$ICONSET/icon_256x256.png" >/dev/null
sips -z 512 512   "$SRC" --out "$ICONSET/frank.g@example.org" >/dev/null
sips -z 512 512   "$SRC" --out "$ICONSET/icon_512x512.png" >/dev/null
sips -z 1024 1024 "$SRC" --out "$ICONSET/walt.e@example.net" >/dev/null

iconutil -c icns "$ICONSET" -o "$ICNS"
cp "$ICNS" "$BRAND/icons/backtester.icns"
touch "$APP"
echo "Wrote $ICNS"
echo "Copied $BRAND/icons/backtester.icns"
echo "Hint: if Dock still shows the old icon, quit the app and run: killall Dock"
