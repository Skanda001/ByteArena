#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$DIR/../.." && pwd)"

cd "$ROOT_DIR"
npx tsx scripts/chaos/dlq-failure.ts
