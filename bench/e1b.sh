#!/usr/bin/env bash
# One more cell of the open-loop study (bench/e1.sh): a reader that moves about 1 MB/s. One job
# alone with 1,024-byte records and Kafka's default fetch size, 8 partitions on one reader, load
# twice the capacity, three repetitions. (Four jobs side by side cannot be fed at this record size.)
#
#   bench/e1b.sh
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e1b starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --reps 3 --scenarios regime20 --record-bytes 1024 \
    --jobs "builtin+off+agg2+sp1" --out results-e1/p8-r1024 || exit 1
done
echo "### campaign e1b finished: $(date '+%F %T')"
