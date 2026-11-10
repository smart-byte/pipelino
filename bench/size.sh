#!/usr/bin/env bash
# Minified + gzipped size of each contender's main entry, bundled with esbuild (deps included). Run: bash bench/size.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ESB=node_modules/.bin/esbuild
printf '| package | version | min | min+gz |\n|---|---|---:|---:|\n'
for pkg in pipelino p-queue fastq async p-limit; do
  entry=$([ "$pkg" = pipelino ] && echo dist/index.js || echo "$pkg")
  version=$(node -p "require('./$([ "$pkg" = pipelino ] && echo package.json || echo node_modules/$pkg/package.json)').version")
  min=$("$ESB" "$entry" --bundle --minify --format=esm --platform=browser --log-level=error | wc -c | tr -d ' ')
  gz=$("$ESB" "$entry" --bundle --minify --format=esm --platform=browser --log-level=error | gzip -9 | wc -c | tr -d ' ')
  printf '| %s | %s | %s B | %s B |\n' "$pkg" "$version" "$min" "$gz"
done
