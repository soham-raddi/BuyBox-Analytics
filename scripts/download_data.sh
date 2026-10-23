#!/usr/bin/env bash
# Downloads the Amazon Reviews 2023 (All_Beauty) files into data/raw/ if they are not there yet (~540 MB).
set -euo pipefail
RAW="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
BASE="https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw"
mkdir -p "$RAW"
for f in review_categories/All_Beauty.jsonl meta_categories/meta_All_Beauty.jsonl; do
  out="$RAW/$(basename "$f")"
  if [ -s "$out" ]; then echo "Already present: $out"; continue; fi
  echo "Downloading $(basename "$f") ..."
  curl -fL --retry 3 -o "$out.part" "$BASE/$f" && mv "$out.part" "$out"
done
ls -lh "$RAW"
