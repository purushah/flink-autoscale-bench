#!/usr/bin/env bash
# Open-loop study of the backlog signal: the job does not scale, the load exceeds its capacity for
# three minutes, and four jobs side by side differ only in the Kafka consumer's fetch size. The
# grid varies the number of partitions per reader, the record size and the overload ratio.
#
#   bench/e1.sh            # about 2.5 hours; finished runs are skipped
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-results-e1}"

run() {  # run <config name> <scenario> <reps> <source parallelism> [extra bench.py arguments]
  local name="$1" scenario="$2" reps="$3" sp="$4"; shift 4
  local base="builtin+off+agg2+sp${sp}"
  python3 bench/bench.py all --reps "$reps" --scenarios "$scenario" \
    --jobs "${base}+f4096,${base}+f65536,${base}+f262144,${base}" --out "$OUT/$name" "$@" || exit 1
}

for pass in 1 2; do
  # Core grid, three repetitions: 8 or 1 partitions per reader, 40-byte or 256-byte records, load = 2 x capacity.
  # (With 1 KB records the load generator cannot deliver 2000 records/s to four topics.)
  run p8-r40   regime20 3 1
  run p1-r40   regime20 3 8
  run p8-r256  regime20 3 1 --record-bytes 256
  run p1-r256  regime20 3 8 --record-bytes 256
  # Extended grid, one repetition: 4 partitions per reader; load 1.5 x and 3 x capacity; 64 partitions on one reader.
  run p4-r40   regime20 1 2
  run p8-r40   regime15 1 1
  run p8-r40   regime30 1 1
  run p64-r40  regime20 1 1 --partitions 64
done
echo "### e1 finished: $(date '+%F %T')"
