#!/usr/bin/env bash
set -e

echo "=== Running ByteArena Benchmark ==="
npx tsx scripts/bench/benchmark.ts "$@"
