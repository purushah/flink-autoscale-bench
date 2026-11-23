#!/usr/bin/env bash
# Tenth chain (2026-11-19). Replaces chain 9, which was never started. Started by hand once the VM
# is back (it was stopped by hand on 2026-11-19 12:28 PST, ISSUES.md #90).
#
# Order, by what the paper and the open question about other studies' load shapes need first:
#   lit, default arm, repetition 1   the load shapes of other autoscaling studies (6 runs)
#   main to 6 repetitions            resumes the main campaign; finished runs are skipped (to 60 runs)
#   lit, accurate arm, repetition 1  the same 6 shapes with the fresh backlog signal
#   e3, e3b, e8s, e5                 as in chain 8
#   main to 8 repetitions            the last 20 runs of the main campaign
#   lit, repetition 2
#   e9, e4, repeats of e2 and e8 (runs in which a JobManager was killed), e5b
# The main campaign is analysed only when it has ended, at 80 runs or, if time runs out, at the last
# complete repetition of both arms (paper/outline.md).
# BENCH_HOURS (for example 18-07) limits the hours in which a run may start; unset means no limit.
cd "$(dirname "$0")/../.." || exit 1
if pgrep -f "bench/bench\.py" > /dev/null || pgrep -f "bench/campaign" > /dev/null; then
  echo "a bench process is running, not starting: $(date '+%F %T')"; exit 1
fi
if ! kubectl --context kind-jev-bench get --raw /readyz --request-timeout=10s > /dev/null 2>&1; then
  echo "the cluster does not answer, not starting: $(date '+%F %T')"; exit 1
fi
[ -s campaign-main.log ] && mv campaign-main.log "campaign-main-part1-$(date +%m%d-%H%M%S).log"
bad=0
note() {  # $1 log tag: count its failed runs; two bad campaigns in a row stop the chain
  failed=$(grep -c "^!!! " "campaign-$1.log")
  echo "$1 over, failed runs $failed: $(date '+%F %T')"
  if grep -qs "does not answer; stopping the campaign" "campaign-$1.log"; then
    echo "stopping the chain: the cluster is gone"; exit 1
  fi
  if [ "$failed" -gt 2 ]; then bad=$((bad + 1)); else bad=0; fi
  if [ "$bad" -ge 2 ]; then echo "stopping the chain: two campaigns in a row with failed runs"; exit 1; fi
}
run() {  # $1 campaign, $2 log tag (default: the campaign's name); settings come from the environment
  local tag="${2:-$1}"
  bench/campaign.sh "$1" > "campaign-$tag.log" 2>&1; note "$tag"
}
own() {  # $1 name, $2 launcher, $3 pattern of its bench processes; give it up after two failed runs
  "$2" > "campaign-$1.log" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$(grep -c "^!!! " "campaign-$1.log")" -ge 2 ]; then
      echo "$1: two failed runs, giving it up: $(date '+%F %T')"
      pkill -TERM -P "$pid"; kill -TERM "$pid"; sleep 5; pkill -KILL -f "$3" 2>/dev/null
      break
    fi
    sleep 30
  done
  wait "$pid" 2>/dev/null
  note "$1"
}
echo "chain 10 starts: $(date '+%F %T') (BENCH_HOURS=${BENCH_HOURS:-unset})"
REPS_LIT=1 LIT_ARMS=default run lit lit-default1
REPS=6 run main main-part2
REPS_LIT=1 run lit lit-rep1
run e3
own e3b bench/campaign_e3b.sh "results-e3b/"
own e8s bench/campaign_e8s.sh "results-e8s/"
run e5
REPS=8 run main main-part3
REPS_LIT=2 run lit lit-rep2
run e9
run e4
# One job alone (e2): a JobManager was killed in 5 of the 6 runs. All 6 are repeated.
if [ -d results-e2/solo-builtin ] && [ ! -d results-e2-jm1g/solo-builtin ]; then
  mkdir -p results-e2-jm1g
  mv results-e2/solo-builtin results-e2/solo-rule results-e2-jm1g/ && mv campaign-e2.log campaign-e2-jm1g.log
fi
run e2
# Short scenarios (e8): the 4 runs in which a JobManager was killed are repeated into results-e8-repeat/;
# the 24 original runs stay in results-e8/ and remain the reported result.
own e8r bench/campaign_e8r.sh "results-e8-repeat/"
run e5b
echo "chain finished: $(date '+%F %T')"
