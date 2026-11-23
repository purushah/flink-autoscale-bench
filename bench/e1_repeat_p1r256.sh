#!/usr/bin/env bash
# Repeat of one cell of the open-loop grid (one partition per reader, 256-byte records). In the second
# run of its first execution two jobs failed over inside the overload (ISSUES.md #86), so the cell is
# repeated in full. Move the first execution away before starting this:
#
#   mkdir -p results-e1-first && mv results-e1/p1-r256 results-e1-first/p1-r256
#   bench/e1_repeat_p1r256.sh
#
# Same arguments as the cell's line in bench/e1.sh. Finished runs are skipped; the second pass retries
# failed ones.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-results-e1}"
base="builtin+off+agg2+sp8"
echo "### e1 cell p1-r256, repeat, starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --reps 3 --scenarios regime20 \
    --jobs "${base}+f4096,${base}+f65536,${base}+f262144,${base}" --out "$OUT/p1-r256" --record-bytes 256 || exit 1
done
echo "### e1 cell p1-r256, repeat, finished: $(date '+%F %T')"
