#!/usr/bin/env bash
# The benchmark campaigns of the paper. Finished runs are skipped, so a campaign can be stopped and
# started again; a second pass retries runs that failed in the first (for example because the
# machine slept).
#
#   bench/campaign.sh main      # 4 controllers x 2 backlog-signal arms x 5 scenarios x REPS repetitions
#   bench/campaign.sh e3        # the built-in rule with other bands, next to the policy in code
#   bench/campaign.sh e2        # noise floor: identical jobs side by side, and single jobs alone
#   bench/campaign.sh e2c       # noise floor in the three scenarios that had none: four identical
#                               # policy-in-code jobs in skew, ramp and drop, repetitions 1 to 8
#   bench/campaign.sh e4        # restart-cost sensitivity: every (re)start takes a minute longer
#   bench/campaign.sh e5        # ten held-out workloads drawn at random (e5b: fresh signal, long windows)
#   bench/campaign.sh e9        # fault injection: the model provider fails or hangs during a spike
#   bench/campaign.sh e8        # the built-in rule with the default backlog signal, with the backlog
#                               # taken from the broker (no job change), and with a small fetch size
#   bench/campaign.sh e8b       # the same question in a sustained overload, for the stock and the
#                               # matched band
#   bench/campaign.sh e10       # the four controllers of the main campaign in the sustained overload
#   bench/campaign.sh lit       # the four controllers under load shapes that other autoscaling studies
#                               # test with: outage and replay, steady, on-off, random walk, daily cycle,
#                               # rate levels in random order (LIT_ARMS=default runs one arm only)
#   bench/campaign.sh main e3 e2 e4   # one after the other
#
# In every campaign the source runs at a fixed parallelism of 2 (4 partitions per reader) for all
# jobs, and the scenario's changes are delayed by a random time of up to 60 s per run.
set -uo pipefail
cd "$(dirname "$0")/.."

COMMON=(--source-parallelism 2 --jitter 60)
MATCHED="builtin+max70"   # the built-in rule with its scale-up bound at the target, as the engine has it
FETCH_BYTES="${FETCH_BYTES:-4096}"

bench() { python3 bench/bench.py all "${COMMON[@]}" "$@" || exit 1; }

main() {
  local reps="${REPS:-8}" jobs="$MATCHED,rule,jev,llm" scenarios="ramp,spike,scale_down,skew,capacity"
  for pass in 1 2; do
    for rep in $(seq 1 "$reps"); do
      # Alternate which arm runs first, so that neither arm is always the earlier one.
      local first=default second=accurate
      if (( rep % 2 == 0 )); then first=accurate; second=default; fi
      for arm in "$first" "$second"; do
        echo "### main pass $pass, repetition $rep, arm $arm: $(date '+%F %T')"
        if [[ "$arm" == default ]]; then
          bench --reps "$rep" --jobs "$jobs" --scenarios "$scenarios" --out results-v3/default
        else
          bench --reps "$rep" --jobs "$jobs" --scenarios "$scenarios" --fetch-bytes "$FETCH_BYTES" --out results-v3/accurate
        fi
      done
    done
  done
}

e3() {
  for pass in 1 2; do
    bench --reps "${REPS_E3:-5}" --jobs "builtin,builtin+max80+min50,$MATCHED,rule" \
      --scenarios spike,ramp,scale_down,capacity --out results-e3/default
  done
}

e2() {
  for pass in 1 2; do
    # Identical built-in jobs, two of them with the observe-only plugin and two without any plugin.
    bench --reps 5 --jobs "$MATCHED+a,$MATCHED+b,$MATCHED+noplugin+c,$MATCHED+noplugin+d" \
      --scenarios spike,capacity --out results-e2/aa-builtin
    # Identical policy-in-code jobs.
    bench --reps 3 --jobs "rule+a,rule+b,rule+c,rule+d" --scenarios spike,capacity --out results-e2/aa-rule
    # One job alone on the cluster.
    bench --reps 3 --jobs "$MATCHED" --scenarios spike --out results-e2/solo-builtin
    bench --reps 3 --jobs "rule" --scenarios spike --out results-e2/solo-rule
  done
}

e2c() {
  for pass in 1 2; do
    # Identical policy-in-code jobs in the scenarios of the main campaign that E2 does not cover. The
    # delay of the load change, the hot keys and the job order follow from scenario and repetition, so
    # repetitions 1 to 8 have the draws of the main campaign (plan entry of 2026-11-22 09:54 PST).
    bench --reps "${REPS_E2C:-8}" --jobs "rule+a,rule+b,rule+c,rule+d" --scenarios skew,ramp,scale_down \
      --out results-e2c/aa-rule
  done
}

e4() {
  for pass in 1 2; do
    # Every (re)start of the Aggregate operator takes 60 s longer; all controllers are told that a
    # restart takes 70 s.
    bench --reps 5 --jobs "$MATCHED+rd60+rt70,rule+rd60+rt70,jev+rd60+rt70,llm+rd60+rt70" \
      --scenarios spike_long,ramp_long --out results-e4/default
  done
}

e8() {
  for pass in 1 2; do
    bench --reps "${REPS_E8:-8}" --jobs "$MATCHED,$MATCHED+lagfix,$MATCHED+f4096,builtin" \
      --scenarios spike,ramp,capacity --out results-e8/default
  done
}

e8b() {
  for pass in 1 2; do
    # A sustained overload that needs more than one doubling: stock and matched band, each with the
    # default backlog signal and with the backlog taken from the broker.
    bench --reps "${REPS_E8B:-8}" --jobs "builtin,builtin+lagfix,$MATCHED,$MATCHED+lagfix" \
      --scenarios surge --out results-e8b/default
  done
}

e10() {
  # The four controllers of the main campaign in the sustained overload, with the default and with
  # the fresh backlog signal; arms alternate as in the main campaign.
  local reps="${REPS_E10:-8}" jobs="$MATCHED,rule,jev,llm"
  for pass in 1 2; do
    for rep in $(seq 1 "$reps"); do
      local first=default second=accurate
      if (( rep % 2 == 0 )); then first=accurate; second=default; fi
      for arm in "$first" "$second"; do
        if [[ "$arm" == default ]]; then
          bench --reps "$rep" --jobs "$jobs" --scenarios surge --out results-e10/default
        else
          bench --reps "$rep" --jobs "$jobs" --scenarios surge --fetch-bytes "$FETCH_BYTES" --out results-e10/accurate
        fi
      done
    done
  done
}

lit() {
  # Load shapes taken from the evaluation sections of other autoscaling studies (research report
  # "Autoscaling paper test scenarios"): the four controllers of the main campaign, with the default
  # and with the fresh backlog signal; arms alternate as in the main campaign.
  # The shortest runs come first, so that a fault in a new scenario shows early.
  local reps="${REPS_LIT:-2}" jobs="$MATCHED,rule,jev,llm" arms="${LIT_ARMS:-default accurate}"
  local scenarios="${SCENARIOS_LIT:-replay,steady,onoff,walk1,cycle,levels1}"
  for pass in 1 2; do
    for rep in $(seq 1 "$reps"); do
      local first=default second=accurate
      if (( rep % 2 == 0 )); then first=accurate; second=default; fi
      for arm in "$first" "$second"; do
        [[ " $arms " == *" $arm "* ]] || continue
        echo "### lit pass $pass, repetition $rep, arm $arm: $(date '+%F %T')"
        if [[ "$arm" == default ]]; then
          bench --reps "$rep" --jobs "$jobs" --scenarios "$scenarios" --out results-lit/default
        else
          bench --reps "$rep" --jobs "$jobs" --scenarios "$scenarios" --fetch-bytes "$FETCH_BYTES" --out results-lit/accurate
        fi
      done
    done
  done
}

e9() {
  for pass in 1 2; do
    # The model provider fails for all jobs during the spike: first with errors, then by hanging.
    bench --reps 3 --jobs "$MATCHED,rule,jev,llm" --scenarios spike --fault error,0,180 --out results-e9/error
    bench --reps 3 --jobs "$MATCHED,rule,jev,llm" --scenarios spike --fault hang,0,180 --out results-e9/hang
  done
}

e5() {
  # Held-out workloads: drawn at random (bench/scenarios.py, random_scenario) after the plugin, the
  # prompts and all thresholds were frozen. One run per workload; the workload is the unit.
  local jobs="$MATCHED,rule,jev,llm"
  for pass in 1 2; do
    bench --reps 1 --jobs "$jobs" --out results-e5/default \
      --scenarios random101,random102,random103,random104,random105,random106,random107,random108,random109,random110
  done
}

e5b() {
  local jobs="$MATCHED,rule,jev,llm"
  for pass in 1 2; do
    # The first five workloads again with the fresh backlog signal.
    bench --reps 1 --jobs "$jobs" --fetch-bytes "$FETCH_BYTES" --out results-e5/accurate \
      --scenarios random101,random102,random103,random104,random105
    # Four other workloads, four times slower, with 5-minute windows (one hour each).
    bench --reps 1 --jobs "$jobs" --windows long --out results-e5/long \
      --scenarios random201x,random202x,random203x,random204x
  done
}

for campaign in "$@"; do
  echo "### campaign $campaign starts: $(date '+%F %T')"
  "$campaign"
  echo "### campaign $campaign finished: $(date '+%F %T')"
done
