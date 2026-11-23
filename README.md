# Model-driven autoscaling for Flink Kubernetes Operator 1.16: a study harness

> **About this copy.** This repository is the public copy of the working folder of a study of
> autoscaling for Apache Flink on Kubernetes.
>
> - It holds the test harness (`bench/`), the two autoscaler plugins (`jev-autoscaler-plugin/` for the
>   laptop testbed, `cluster/plugin/` for the shared cluster), the test jobs, the analysis scripts, and
>   all raw data of the laptop campaigns (`results*/`, `testbed/`, the campaign and chain logs).
> - For the tests on the shared cluster it holds the extracts of the monitoring service
>   (`results-cluster/*/datadog*.json`). The cluster's configuration and the raw files of those runs
>   are left out, because they hold names of the cluster.
> - Also left out: the paper's sources, build outputs, and the working tracker. Numbers such as
>   "ISSUES.md #105" in the text refer to that tracker.
> - Replaced in every file: local paths (`/path/to/repo`), the image registry
>   (`registry.example.com`), one host name (`host.example.com`), the names of unrelated containers on
>   the test laptop (`other-container-N`), and the cluster's name (`cluster`, also in file names,
>   folder names and image tags). Checksums quoted in the text are those of the files before these
>   replacements.

This repository holds a plugin for the autoscaler of Flink Kubernetes Operator 1.16.1, a benchmark,
and the analysis behind a study of two questions:

1. Does a model that chooses the parallelism of every vertex do better than the operator's rule?
   Two kinds of model are tried: a decision model (TypeSafe AI Jev) and an LLM (Claude).
2. How good is the backlog signal the autoscaler works with? It turned out to be the larger issue.

The plugin is a harness for the study, not a product. Findings are in [FINDINGS.md](FINDINGS.md)
(being rewritten while the campaigns run), every defect found on the way is in [ISSUES.md](ISSUES.md).

## What is in here

| Path | Contents |
|---|---|
| `jev-autoscaler-plugin/` | The operator plugin (Java 17): decision engine with the sizing policy and limits, Jev client, Claude client, evaluator and executor plugins, broker-side backlog, a replay tool, unit tests |
| `flink-job/` | The benchmark job (Flink 1.20, Kafka source and sink, keyed state, paced per-record work) |
| `deploy/` | kind cluster, Kafka, operator Helm values, image Dockerfiles |
| `bench/` | Load generator, sampler and run logic (`bench.py`), scenarios (`scenarios.py`), campaigns (`campaign.sh`, `e1.sh`), analysis (`analyze.py`, `paper.py`, `paper_e1.py`), offline and replay studies (`model_eval.py`, `replay_states.py`, `replay_agree.py`) |
| `scripts/` | `setup.sh`, `build.sh`, `teardown.sh` |
| `cluster/` | The same benchmark on a shared cluster: harness, queues, the plugin with four more settings, the job for Flink 2.x. See "The cluster test" below |
| `results*/` | Raw data per run. See "Results directories" below for which ones are current |
| `results-cluster/` | The cluster test: per campaign the numbers from the cluster's monitoring service (`datadog.json`). The harness files and saved logs next to them hold values of the namespace and are not published |

## The plugin

Operator 1.16 has two autoscaler plugin interfaces. The plugin implements both, because the second
one alone cannot start a scaling action.

1. **`ScalingMetricsEvaluatorPlugin`** (`JevEvaluatorPlugin`) is called per vertex in every cycle.
   On the last vertex it runs the decision engine once for the whole job. If the plan changes a
   vertex, it overrides that vertex's metrics so that the built-in logic raises a scaling action in
   the same cycle.
2. **`ScalingExecutorPlugin`** (`JevExecutorPlugin`) is called when a scaling action is about to be
   applied. It replaces the built-in numbers with the plan, drops the action if the plan is to
   hold, or passes the built-in decision through when no plan is usable.

### Who decides (`job.autoscaler.jev.provider`)

| Value | Benchmark job spec | What chooses the parallelism |
|---|---|---|
| `rule` | `rule` | The policy in code. This is the control for the two models |
| `jev` (default) | `jev` | Jev reads a menu with one option per allowed parallelism; code has computed a verdict for every option |
| `anthropic` | `llm` | A Claude model gets the measurements and the same policy in words and returns one number per vertex |
| `observe` | `builtin` | Nothing: the operator's rule decides, the plugin only logs what the rule believes |

All of them use one sizing formula, the built-in rule's, with the job's own autoscaler settings:

```
trigger = ceil((demand / target + backlog / catch-up) / capacity per subtask)
size    = ceil((demand / target + backlog / catch-up + demand * restart / catch-up) / capacity per subtask)
scale up to size if parallelism < trigger; scale down to size if parallelism - size >= 2; else hold
```

Only parallelisms the built-in rule would also use are offered (divisors of the partition count for
a source, of the key groups for a keyed vertex). Vertices excluded from autoscaling are never asked
about. Every decision is one JSON line in the operator log starting with `JEV_DECISION`, with the
policy's choice (`rule_choice`) next to the model's.

### Limits (enforced in `DecisionEngine`, covered by unit tests)

| Rule | Default | Setting (`job.autoscaler.jev.` prefix) |
|---|---|---|
| Per-vertex parallelism limits | built-in min/max | `min-parallelism`, `max-parallelism` |
| Largest single scale-up / scale-down | ×4 / half | `max-scale-up-factor`, `max-scale-down-factor` |
| Utilization after a scale-down | at most 85% (projected) | `max-utilization-after-scale-down` |
| Scale-down while the backlog grows or its trend is unknown | not offered | – |
| Wait after a scaling action, at all / to scale down | 45 s / 150 s | `cooldown.scale-up`, `cooldown.scale-down` |
| Probability to scale up / down (Jev only), and the top option must point the same way | 0.5 / 0.7 | `min-up-probability`, `min-down-probability`, `require-top-choice` |
| Request timeout and retries | 4 s (Jev), 8 s (Claude), 2 retries | `request-timeout`, `llm.request-timeout`, `max-retries` |
| Circuit breaker | 3 failed cycles, open 120 s | `circuit-breaker.failures`, `circuit-breaker.open` |

On a timeout, an error, an unusable answer, a missing key or an open circuit, the built-in decision
is applied within the same limits. API keys are read from environment variables of the operator pod
(`TYPESAFE_API_KEY`, `ANTHROPIC_API_KEY`). A job cannot redirect the endpoint or the key variable
unless the operator is started with `ARBITER_ALLOW_JOB_ENDPOINTS=true`.

### Backlog from the broker (`job.autoscaler.jev.lag.source: broker`, job spec option `+lagfix`)

Flink's `pendingRecords` under-reports the backlog of a backpressured Kafka reader (Issue #15). In
this mode the evaluator replaces a source's backlog and demand with values from the broker: end
offsets through the Kafka admin API (`ARBITER_KAFKA_BOOTSTRAP` on the operator) minus the readers'
positions from Flink's `currentOffset` gauges, and the growth of the end offsets as the arrival rate.
It works with any provider, including `observe`, where it repairs the inputs of the built-in rule
without changing the job.

### Enabling it for a job

The operator image must contain the plugin jar under `/opt/flink/plugins/`.

```yaml
job.autoscaler.enabled: "true"
job.autoscaler.metrics.custom-evaluators: jev
job.autoscaler.metrics.custom-evaluator.jev.class: dev.jevscaler.JevEvaluatorPlugin
job.autoscaler.scaling.custom-executors: jev
job.autoscaler.scaling.custom-executor.jev.class: dev.jevscaler.JevExecutorPlugin
job.autoscaler.jev.provider: rule            # rule | jev | anthropic | observe
job.autoscaler.jev.model: jev-1.14.0         # for provider jev
job.autoscaler.jev.llm.model: claude-haiku-5-5   # for provider anthropic
```

For `observe`, configure the evaluator only.

## The benchmark

Up to four FlinkDeployments run side by side under one operator. The load generator writes the same
records, with the same keys and timing, to one input topic per job, so the jobs of a run are
compared on identical input at the same time.

```
Kafka source  --keyBy-->  Aggregate (keyed state, 500 rec/s per subtask)  -->  Measure + Kafka sink
```

Per-record work is simulated by a paced sleep (the operator accumulates the time it owes and
subtracts the time actually slept), so capacity per subtask is exact and the laptop's CPU is not the
limit.

A job spec is `builtin | rule | jev | llm` plus options joined with `+`, for example
`builtin+max70` (the built-in rule with its upper utilization bound at the target),
`builtin+max70+lagfix`, `rule+rd60+rt70`. `bench/bench.py` lists all options under `SPEC`.

How a run goes:

- Jobs start at the parallelism the sizing formula gives for the scenario's initial load, the same
  for all jobs of a run (Issue #31).
- Warm-up: at least the scenario's warm-up time, until every job has no backlog and has not
  restarted or rescaled for 105 s, so every autoscaler starts with a full metric window.
- The scenario's changes are delayed by a random time per run (`--jitter`), and the order in which
  jobs are deployed is shuffled.
- The sampler reads broker offsets and Flink metrics every 5 s. Flink's REST API answers a metric
  query from the snapshot the previous query asked for, so the sampler asks for a refresh, waits
  one second and then reads (Issue #28).
- A run is discarded and repeated if sampling has gaps, if the load generator fell behind, or if
  the machine slept.

| Scenario | Workload |
|---|---|
| `ramp` | 300 → 1800 rec/s over 4 minutes, then hold |
| `spike` | 400 → 2000 rec/s for 2 minutes, then back |
| `scale_down` | 1800 rec/s dropping to 300 |
| `skew` | 900 rec/s; 60% of records move to 4 hot keys |
| `capacity` | 800 rec/s; per-record cost doubles for 3 minutes |
| `regime15/20/30` | Open loop: a job fixed at 1000 rec/s gets 1.5, 2 or 3 times that for 3 minutes |
| `randomN`, `randomNx` | A workload drawn at random from seed N (`x`: four times slower) |

All jobs share the autoscaler settings in `COMMON_AUTOSCALER` (`bench/bench.py`): 1-minute metric
window, 30 s stabilization, 70% target utilization, 30 s restart time, 1-minute catch-up, parallelism
1 to 8 per vertex. The windows are far shorter than the defaults so that a scenario fits in minutes.

### Campaigns

| Command | What it runs |
|---|---|
| `bench/e1.sh` | Open-loop study of the backlog signal: autoscaler off, four consumer fetch sizes side by side, over partitions per reader, record size and overload ratio |
| `bench/campaign.sh e8` | The built-in rule with the default signal, with the broker-side signal, with a 4 KiB fetch size, and with its default band |
| `bench/campaign.sh e8b` | Sustained overload (scenario `surge`: 200 to 2,400 records/s for 8 minutes, jobs start with one Aggregate subtask): stock band and band at the target, each with the default and the broker-side signal |
| `bench/campaign_e8d.sh` | The same with smaller overloads: 200 to 1,200 records/s (scenario `surge_mid`, 8 repetitions) and 200 to 600 records/s (`surge_low`, 4 repetitions) |
| `bench/campaign_e8c.sh` | The same overload with 5-minute windows (scenario `surge_long`: 200 to 2,400 records/s for 40 minutes, 6 repetitions of about an hour each) |
| `bench/e1b.sh` | One more open-loop cell: one job alone with 1,024-byte records, a reader that moves about 1 MB/s |
| `bench/e1_repeat_p1r256.sh` | Repeat of the open-loop cell with one partition per reader and 256-byte records (a task failure in its first execution, ISSUES.md #86); move the first execution to `results-e1-first/` before |
| `bench/campaign_e2b.sh` | Noise floor in the sustained overload: four identical policy-in-code jobs side by side, 5 repetitions |
| `bench/campaign_e10.sh` | The four controllers of the main campaign in the sustained overload, with the default signal and with the broker-side signal for every controller, 12 repetitions per arm. It replaces `bench/campaign.sh e10`, whose second arm used a 4 KiB fetch |
| `bench/campaign.sh main` | Built-in, policy in code, Jev and Claude side by side; 5 scenarios × 8 repetitions × default and fresh backlog signal |
| `bench/campaign.sh e3` | The built-in rule with other utilization bands next to the policy in code |
| `bench/campaign_e3b.sh` | The sustained overload with the shipped band, the band at the target, the policy in code and the LLM side by side, 6 repetitions |
| `bench/campaign_e8s.sh` | The shipped band with the default signal, the broker-side signal, a 64 KiB and a 4 KiB fetch: the sustained overload (6 repetitions), then spike, ramp and slow-down (8 each) |
| `bench/campaign_e8r.sh` | Repeats of the four runs of `e8` in which a JobManager was killed, into `results-e8-repeat/` |
| `bench/campaign.sh e2` | Noise floor: identical jobs side by side, and one job alone |
| `bench/campaign.sh e4` | Every restart takes 60 s longer |
| `bench/campaign.sh e5`, `e5b` | Held-out workloads drawn at random; also with the fresh signal and with 5-minute windows |
| `bench/campaign.sh e9` | The model provider fails or hangs during a spike |

Never run two benchmark processes at once: each run deletes all FlinkDeployments of the namespace.

A run is refused when the host has less than 8 GiB of disk free. Where Docker runs in a VM, the VM's disk
file keeps the blocks that Kafka and the checkpoints wrote and freed again, so the free space shrinks from
campaign to campaign. `docker exec jev-bench-control-plane fstrim /var` gives it back without deleting
anything (ISSUES.md #108); `bench/chains/chain11.sh` runs it before every campaign.

### Analysis

```bash
python3 bench/paper_e1.py                 # open-loop study: results-e1/e1.csv, table, figures
python3 paper/mk_short_tables.py          # after bench/paper.py and sigsum.py --tex: one-column copies of two tables for the short version, with the cluster rows; the short signal table gives the mean slots of both jobs in place of p and marks the rows with a planned test by a dagger
python3 bench/recover_operator_log.py     # runs whose saved operator log was cut by log rotation; --write rebuilds them (ISSUES.md #69)
python3 bench/job_exceptions.py           # task failures and operator restarts from the operator's log, with the run each fell into (ISSUES.md #82, #86)
python3 bench/failedsched.py results-v3   # pods that could not be placed, from the saved Kubernetes events, inside each run's own window (ISSUES.md #129)
python3 bench/main_marked.py --tex paper/gen/numbers_main_marked.tex   # after `bench/paper.py main`: the planned contrasts without the eight marked runs, by day and by night, pairs at the divisor's floor, margins, and the checks added on 22 Nov (seconds counted from the load change on, runs with a disturbed start, agreement by class, spread per pair, differences in seconds); also writes paper/gen/tab_main_pooled.tex and tab_main_means.tex. Intervals are Student's t intervals for any number of values and the sign-flip tests are exact up to 44 pairs (ISSUES.md #132)
python3 bench/e2c_pairs.py                # after `bench/paper.py e2c`: identical jobs in skew, ramp and drop (campaign `bench/campaign.sh e2c`): the planned contrast's statistic for two jobs that differ in nothing, next to the main campaign's model contrasts on the same scenarios
python3 bench/lit_arm.py results-lit/default --extra results-lit-first/default/onoff/rep1 --csv paper/gen/lit_default.csv   # load shapes of other studies, one arm (no charts); the other arm: results-lit/accurate, paper/gen/lit_accurate.csv
python3 bench/paper.py e8                 # one of: main e8 e8b e8c e8d e8s e10 e3 e3b e2 e2b e2c e4 e5 e9 offline
python3 bench/paper.py signal             # the table over e8, e8b, e8c and e8d; run their analyses first
python3 bench/analyze.py --results DIR    # per-run metrics and charts for one results directory
```

`paper.py` writes tables, figures and every quoted number (as LaTeX macros) to `paper/gen/` and
`paper/figs/`, and a plain-text digest to `paper/gen/summary_<campaign>.txt`.

The paper builds in two versions from one source:

```bash
cd paper
tectonic -X compile main.tex        # short version (the default)
tectonic -X compile main-long.tex   # long version: also contains everything inside \iflong ... \fi
```

Text and floats that only the long version carries are wrapped in `\iflong ... \else ... \fi` in
`paper/sections/`. Copies of the sections from before each round of cuts are in `paper/uncut/`.

### Results directories

| Directory | Status |
|---|---|
| `results-e1/`, `results-e8/`, `results-e8b/`, `results-e8c/`, `results-e8d/`, `results-e8s/`, `results-e10/`, `results-v3/`, `results-e2/` to `results-e9/`, `results-e2b/`, `results-e3b/`, `results-e8-repeat/` | Current campaigns |
| `testbed/operator-logs/`, `testbed/oom_kills.txt` | Copies of the operator's log files from the node (the kubelet rotates them) and the kernel's record of out-of-memory kills |
| `testbed/jm-follow/` | Scheduler lines of the test jobs' JobManager logs (restarts, released workers, closed TaskExecutor connections), copied by `bench/chains/jmfollow.sh` while a queue runs; `python3 cluster/bench/tmstops.py testbed/jm-follow` counts restarts and TaskManagers that left by themselves (ISSUES.md #105) |
| `results-e8c-first/`, `results-e1-first/` (from 2026-11-19) | First executions of the long-window campaign and of one open-loop cell, repeated because a task failed inside the overload (ISSUES.md #86) |
| `results-e8b-jm1g/`, `results-e8d-jm1g/` (later also `results-e2-jm1g/`, `results-e8-jm1g/`) | Runs from before the JobManager's container got more memory (ISSUES.md #61): the first execution of the overload campaign and runs in which a JobManager was killed. Kept for the record; `python3 bench/paper.py e8b --root results-e8b-jm1g --out <dir>` analyses the first execution. |
| `results/model_eval_random/`, `results-replay/` | Offline study and replay of logged states |
| `results-e1-stale/` | First open-loop grid, sampled before Issue #28 was fixed. Kept for reference |
| `results/`, `results-3way/`, `results-pilot-v2/`, `results-e1-invalid/` | Pilots. They predate Issues #16, #17, #28 and #31 and are not used for claims |

## The cluster test (`cluster/`)

The same job, scenarios and controllers on a shared Kubernetes cluster: Flink 2.3, operator 1.16.1, a
managed Kafka service, checkpoints in an object store and the platform's metric pipeline. `--scale K`
multiplies every rate, the partitions, the keys and the largest parallelism by K (2 in the reported
runs: 16 subtasks per vertex). The test is exploratory. Its results are sections 7 to 10 of
[FINDINGS.md](FINDINGS.md).

| Path | What |
|---|---|
| `cluster/bench/bench_cluster.py` | The harness. It imports `bench/bench.py`, runs in a pod in the namespace under test and only touches objects that carry its label (`asbench=true`). Per run it also keeps the logs of the jobs' own pods and of the operator (`logs/`) and records in `meta.json` the share of seconds with latency data per job and the TaskManagers that stopped with a fatal error |
| `cluster/bench/queue.sh`, `queue_announced.sh`, `queue_notes.sh`, `queue_flapping.sh`, `queue_falsealarm.sh` | The queues: the four controllers, a spike that is announced, other wordings of the announcement, a load that falls and comes back, an announced spike that does not come (`--no-event`) |
| `cluster/bench/expsum.py`, `flapsum.py`, `flapphase.py`, `falsesum.py` | Summaries from the harness files of a campaign or of single runs |
| `cluster/bench/tmstops.py`, `ckrace.py` | Scans of JobManager logs for TaskManagers that stopped by themselves (ISSUES.md #105) |
| `cluster/bench/fixcheck.py` | The same check per run from the logs the harness saves in `<run>/logs/`: restarts, TaskManagers gone without a release request, forced producer closes, close errors of the sink's writer and fatal exits |
| `cluster/bench/queue_policyfix.sh` | The flapping loads with the two additions to the policy of ISSUES.md #107 (`scale-down.window-caps-trend`, `scale-down.min-slots` in `cluster/plugin`): built-in, policy, policy with look-back, the same with both additions, LLM with history and the slot rule |
| `cluster/bench/queue_pretrend.sh`, `flaporders.py` | A test of the cause of ISSUES.md #119 that needs no new operator image: on-off load with a fifth job that has the look-back and its trend projection switched off by a setting (`--arm rule+d=window:600,trend:0`; the queue stops before a run if the registered tests of the fixes can start). `flaporders.py` lists every order for Aggregate in the decision log of the policy jobs, split into the first high phase and the later ones, and for logs that carry the earlier level it checks the cap (B2, B5 of the plan) |
| `cluster/bench/flapwhen.py` | On-off runs: how many scale-downs came into effect in a high phase and how many scale-ups in a low phase (the changes of `flapsum.py`) |
| `cluster/bench/queue_stepfix.sh`, `stepreplay.py`, `stepscore.py` | The step-aware demand of the broker-side signal (`lag.step-aware`, ISSUES.md #123): ten eight-minute overloads with and without it side by side, and a replay of recorded runs that shows what it would have reported at each evaluation. `python3 cluster/bench/stepscore.py results-cluster/stepfix` scores the test's predictions A1 to A5 from the operator logs, the harness files and the monitoring numbers. With five-minute windows (Test C): `sh queue_stepfix.sh 2 6 surge_long long` in the driver pod, scored by `python3 cluster/bench/stepscore.py results-cluster/stepfix surge_long` (C1 to C7). The replay takes the window length from each run's `meta.json`. |
| `cluster/bench/signalcheck.py` | What `pendingRecords` shows of the true backlog (end offsets minus reader positions) in cluster runs, per job-run (FINDINGS.md section 12); `--by-size` splits it by the parallelism the job runs at (section 13) |
| `cluster/bench/decount.py` | Model against policy by the plan's definition (measured phase, model asked, most likely option) for any run folder, cluster or laptop |
| `cluster/bench/tmdrop.py` | TaskManagers lost at a restart, found in the samples alone (pods fall while the job asks for more); `results-v3` and `results-e5` are skipped while they run |
| `bench/disturbed.py` | Laptop runs measured next to another workload on the host (ISSUES.md #125): applies the plan's criterion to run folders from `testbed/interference.log` and `testbed/restarts.log`; reads no outcome: `python3 bench/disturbed.py results-e5/default/random110/rep1` |
| `cluster/bench/queue_signal.sh`, `sigsum.py`, `scalesteps.py` | The backlog signal in closed loop on the cluster (the eight-minute overload with the built-in rule, both bands, default and broker-side signal), its paired analysis (`--tex` writes the paper's macros to `paper/gen/numbers_clsig.tex`) and the list of scale-ups the rule ordered, from the operator's log (FINDINGS.md section 13); the plugin of `cluster/plugin` takes the broker list and the login from the job's settings (`lag.bootstrap-servers`, `lag.auth: msk-iam`) The long-window campaign: `python3 cluster/bench/sigsum.py results-cluster/signal surge_long --one-test --tex paper/gen/numbers_clsiglong.tex ClSigLong` (`--one-test`: the plan names one test there, no correction). `sigsum.py` also prints what the rule's formula asked for against the maximum of 16 subtasks (the plugin logs the formula's size before the maximum at every evaluation, `rule_size` in `decisions.jsonl`; not planned), and writes four-digit macro values with a thousands separator. |
| `cluster/bench/queue_defaultwin.sh` | The backlog signal in closed loop on the cluster at the operator's default time settings (`bench_cluster.py --windows default`: 15-minute metric window, 5-minute stabilization, 30-minute catch-up, 5-minute restart time, 1-hour scale-down interval; built-in jobs only). Scenario `surge_default`, defined in `bench_cluster.py`: a warm-up of 21 minutes, then 4,800 records/s at scale 2 for 90 minutes; about 2 h 10 min per run. The queue waits for the registered tests of the fixes whenever their image is there, and starts no run after the end epoch given as its third argument. Analysis: `sigsum.py results-cluster/defaultwin surge_default`, and `defaultscore.py results-cluster/defaultwin` for the six predictions of the plan (orders and their times, the time at which a job has 16 subtasks, the share of the backlog that `pendingRecords` shows in the first ten minutes) |
| `cluster/bench/podscan.py`, `cluster/bench/disturbed_cluster.py` | Runs in which the cluster moved a pod of a job. The driver pod's results folder is an `emptyDir` and the cluster replaces pods from time to time (ISSUES.md #135), so the pods' logs never leave the pod: `podscan.py <run dir>` runs in the driver pod before a run is copied out and writes `disturb.json` (the JobManager pods the run saw, and every TaskManager whose connection the JobManager closed without a release request). `python3 cluster/bench/disturbed_cluster.py results-cluster/<campaign> [scenario]` on the laptop marks a run as disturbed if that happened between the first and the last change of the load. Neither reads latency or sizes. |
| `cluster/bench/rebuild_decisions.py` | Rebuilds a run's decision log from the operator log that the harness followed during the run, when the log fetched at the end was cut by rotation (ISSUES.md #109) |
| `cluster/bench/oplog.py` | The operator's log of a cluster run from both copies the harness keeps (the log fetched at the end, which can start late after a rotation, and the log followed during the run), each line once and in time order; used by `scalesteps.py` and `stepscore.py` (ISSUES.md #126). `python3 cluster/bench/oplog.py <run dir>` prints what each copy covers |
| `cluster/bench/stepscan.py` | The step-aware demand (ISSUES.md #123), checked for false alarms on recorded signal runs: applies the option's condition at every evaluation of every job with the broker-side signal and counts the steps before the rise, right after it, later in the overload and after the fall: `python3 cluster/bench/stepscan.py results-cluster/signal/surge/rep1 ...` |
| `cluster/bench/capreplay.py` | The cap on the trend projection (ISSUES.md #119), replayed on recorded on-off runs: for every scale-up the jobs with the look-back ordered from the second high phase on, the policy's threshold as logged, with the cap as first built, and with the cap at the earlier level itself; reads the decision logs only: `python3 cluster/bench/capreplay.py results-cluster/flapping/onoff/rep1 ...` |
| `cluster/bench/readfail.py` | Check without any outcome: did the plugin fail to read the backlog from the brokers between the two load changes of a cluster run (the operator's line "Could not read the backlog from the broker")? Run from `cluster/bench`: `python3 readfail.py ../../results-cluster/defaultwin/surge_default/rep*` |
| `cluster/bench/stepnow.py`, `steptex.py` | The step lines of the step-aware demand against the load that was sent (newest interval, the reading within the cycle, the demand; ISSUES.md #141), and the paper's macros for Tests C and A: `python3 cluster/bench/stepnow.py results-cluster/stepfix/surge/rep*`. `queue_stepfix2.sh` is the queue of the check of the remedy (four jobs that all have the option, operator image with the reading over two seconds) |
| `cluster/bench/offprobe.py` | A probe without Flink and without the operator: one topic, a load of known rate, and the end offsets read over 0.45, 1 and 2 s with three ways of stamping a reading. It shows how far a rate between two readings of the end offsets is from the true rate (ISSUES.md #141; FINDINGS.md section 16). `python3 cluster/bench/offprobe.py out.csv` uses the laptop's broker; with `--conf <cluster settings>` it logs in as the harness does. It creates and deletes a topic of its own and sends its own load: do not run it next to a measured run. Readings: `results-cluster/offprobe/` |
| `cluster/bench/beliefs.py` | What the rule believed (demand and backlog) at its first evaluation on a full window of overload in the cluster signal campaign, against the truth: `python3 cluster/bench/beliefs.py results-cluster/signal surge 60 125` |
| `cluster/bench/keepreplay.py`, `queue_keepfix.sh` | The margin on the size a scale-down keeps (`scale-down.window-keeps-margin`, ISSUES.md #119, second cause): a replay on recorded on-off runs and the queue of its test |
| `bench/e4_start.py`, `bench/e4_blocks.py` | The campaign with every restart 60 s longer: the contrasts by block, and the state of the jobs when the measured phase begins and when the load changes (they had rescaled in the warm-up; ISSUES.md #146): `python3 bench/e4_start.py` |
| `cluster/plugin/` | The plugin with four more settings (below). The laptop campaigns use `jev-autoscaler-plugin/`, which does not have them |
| `cluster/flink-job/` | The benchmark job built for Flink 2.x |
| `cluster/deploy/` | Dockerfiles, `cluster.example.json`, and `make_conf.py`, which derives the cluster's settings and the test's own objects (operator, driver pod, service accounts) from a job that already runs in the namespace |
| `cluster/build.sh` | Builds the three images: job, operator with the plugin, driver |

Which plugin source ran which cluster test. The tests of the two fixes that the paper reports (the cap on
the trend projection and the step-aware demand, 22 and 23 November 2026) ran with the plugin source of
this repository's first commit (6f1a803). The source has changed since in three ways that no test in the
paper has run: the reading of the end offsets within one cycle counts at the first step line of a rise
only; at that line a third reading two seconds after the first replaces it (`lag.step-confirm-seconds`);
and a scale-down that is kept for an earlier level gets a margin (`scale-down.window-keeps-margin`). The
runs at the operator's default time settings switch on none of these options.

Settings that `cluster/plugin` adds (under `job.autoscaler.jev.`) and the harness option that sets each for
one job of a run:

| Setting | `--arm JOB=...` | Effect |
|---|---|---|
| `schedule` (`start,end,rate`: epoch seconds and records/s) | `schedule` | Policy in code: from a lead time before the start (`schedule.lead-seconds`; by default the restart time plus the gap between two decisions) until the end, it sizes for the larger of the measured and the planned rate |
| `llm.notes-base64` | `note`, `note:zone`, `note:relative`, `note:unrelated` | LLM: a note in the job owner's words, passed to the model as it is |
| `scale-down.window-seconds` | `window:600` | Policy in code: a scale-down sizes for the highest demand of the last N seconds |
| `llm.history` | `history` | LLM: the source's rate over the last half hour and what each earlier change cost, with the instruction to hold when the demand goes up and down |

`--planned` fixes the first load change of the scenario on a whole minute before the jobs start, so that
a schedule entry or a note can name it.

```bash
# 1. Images (linux/amd64). REG is your registry; the job's Dockerfile names the platform's Flink image.
(cd cluster/flink-job && mvn -q -DskipTests package) && (cd cluster/plugin && mvn -q package)
REG=registry.example/me TAG=cluster1 PUSH=1 PLUGIN_JAR=cluster/plugin/target/jev-autoscaler-plugin-0.2.0-cluster.jar cluster/build.sh
REG=registry.example/me TAG=cluster1 PUSH=1 cluster/build.sh driver

# 2. Settings of the cluster and the test's objects, derived from a job that runs in the namespace.
#    Both output files hold values of the namespace: do not publish them.
kubectl -n NS get flinkdeployments -o json > fd.json
python3 cluster/deploy/make_conf.py --name mine --namespace NS --flinkdeployments fd.json \
    --reference NAME-OF-A-RUNNING-JOB --registry registry.example/me --tag cluster1
kubectl -n NS apply -f cluster/deploy/objects.mine.yaml

# 3. Keys, as Secrets that only the test's operator reads (key `api-key` in each)
kubectl -n NS create secret generic asbench-jev-api-key --from-literal=api-key="$TYPESAFE_API_KEY"
kubectl -n NS create secret generic asbench-anthropic-api-key --from-literal=api-key="$ANTHROPIC_API_KEY"

# 4. Queues, inside the driver pod (copy cluster/bench/*.sh into the pod first). Finished runs are skipped.
sh queue.sh builtin+max70,rule,jev,llm 2 3      # FINDINGS section 7
sh queue_announced.sh 2 6                       # section 8
sh queue_notes.sh 2 3                           # section 9
sh queue_flapping.sh 2 5 && sh queue_flapping.sh 2 5 levels1    # section 10: on-off load, rate levels
sh queue_falsealarm.sh 2 3                                     # section 8, last part: the announced spike does not come
sh queue_signal.sh 2 8                                         # the backlog signal in closed loop (plan entry of 21 Nov)

# 5. Summaries, from a copy of the pod's results directory
python3 cluster/bench/expsum.py results-cluster/announced
python3 cluster/bench/flapsum.py results-cluster/flapping/onoff/rep*
```

The reported numbers come from the cluster's monitoring service: per run window (`t0_utc` to `end_utc`
in `meta.json`) the sums of the job's two counters `secondsOverLimit` and `secondsMeasured` and the
average of the JobManager's registered and free task slots. They are saved as `datadog.json` next to each
campaign. The harness files give the same picture within 10 s per job and run.

Known problem of the setup, fixed for later runs: with `flink-connector-kafka` 4.0.1 a rescale can stop
TaskManagers, because the Kafka sink rethrows a send error when it is closed during the cancel
(ISSUES.md #105; 11 of 531 restarts in our logs). The job now builds with connector 5.0.0-2.2, whose
callback drops that error; five on-off runs with it had 207 restarts and no such stop
(`results-cluster/fix105/`, `python3 cluster/bench/fixcheck.py <run>...`). All runs before job image `cluster6`
were made with 4.0.1 and are reported with their events named.

## Reproduce

Requirements: Docker with 8 CPUs and 16 GB, `kind`, `kubectl`, `helm`, JDK 17, Maven, Python 3 with
`kafka-python`, `pandas`, `matplotlib`.

```bash
# 1. Keys: only ever read from the environment
export TYPESAFE_API_KEY="$(cat /path/to/your/key.txt)"
export ANTHROPIC_API_KEY="$(cat /path/to/your/other-key.txt)"   # only for llm jobs

# 2. Cluster, Kafka, operator with the plugin (also builds and unit-tests everything)
./scripts/setup.sh

# 3. One run (about 10 minutes) ...
python3 bench/bench.py run --scenario spike --rep 1 --jobs builtin+max70,rule,jev,llm --source-parallelism 2
# ... or a campaign. Finished runs are skipped on rerun. Keep the machine awake and on power.
bench/campaign.sh e8

# 4. Tables, figures and numbers
python3 bench/paper.py e8

# 5. Clean up
./scripts/teardown.sh
```

Unit tests only: `cd jev-autoscaler-plugin && mvn test` (with `JAVA_HOME` pointing to JDK 17).

Offline check of the models against a stated sizing rule (no cluster needed):

```bash
python3 bench/model_eval.py --suite random run --models jev-raw,jev-computed --repeats 5
python3 bench/model_eval.py --suite random run --models haiku,sonnet,sonnet-high,opus,fable --repeats 3
```

Watch decisions live:

```bash
kubectl -n flink-operator logs -f deployment/flink-kubernetes-operator | grep JEV_DECISION
```
