#!/usr/bin/env bash
set -e

echo "=== Running ByteArena Smoke Test ==="
npx tsx scripts/smoke.ts
