# Results of the redesigned campaigns so far (2026-11-22)

Analysed: E1 with its 1 MB/s cell, E8b, E8d, E8c, E8 with E8s, E2 with E2b, E10, E3 with E3b, both
repetitions of the load shapes of other studies (section 6) and the main campaign (section 14, 80 runs).
The remaining campaigns (E5, E9, E4, the repeats of E2 and E8) run at night on the laptop (the testbed was
stopped on 2026-11-19 and restarted that evening, ISSUES.md #90). A test on a shared cluster is in
section 7. See README.md for the list. Predictions were written into `paper/outline.md` before each
campaign ran, and its log says which held.

E8b was run twice. In its first execution the kernel killed a JobManager in 3 of 8 runs (ISSUES.md #61);
the numbers below are from the second execution, after the testbed was repaired. The first is in
`results-e8b-jm1g/` and gave -180 s and -223 s.

## 1. The backlog metric of a Kafka source under-reports under backpressure (E1, 16 runs)

Open loop, autoscaler off, job at twice its capacity for three minutes, four fetch sizes side by side.
Share of the true backlog that `pendingRecords` shows while the job is overloaded:

| Partitions per reader | 1 MiB fetch (default) | 256 KiB | 64 KiB | 4 KiB |
|---|---|---|---|---|
| 64 | 0.3% | 0.4% | 1% | 86% |
| 8 | 3% | 12% | 67% | 96% |
| 4 | 7% | 16% | 68% | 96% |
| 1 | 32% | 44% | 70% | 96% |

(40-byte records, overload ratio 2.) A model built from the client and connector source is within
9 points of every measured cell. One partition per reader does not remove the problem. The testbed
runs Kafka client 3.4.0. From client 4.0 on the fetch rule is stricter, and the model gives 5%
instead of 3% for 8 partitions and the default fetch.
- At about 1 MB/s per reader (1,024-byte records, 8 partitions, 3 runs) the metric showed 80% of the
  backlog; the model had given 76% before the runs. About 21,000 of 188,000 records were unseen at the end.

Data: `results-e1/`. Tables and figures: `paper/gen/tab_e1.tex`, `paper/figs/e1_series.pdf`.

## 2. In a sustained overload the built-in rule acts late and in smaller steps; a broker-side signal fixes it (E8b, 8 runs)

Load steps from 200 to 2,400 records/s for 8 minutes. Jobs start with 1 Aggregate subtask; the rule's
own formula gives 8 for the true load. Four built-in jobs per run: shipped band and band with its upper
bound at the target, each with the default signal and with backlog and arrival rate read from the broker.

| Band, signal | Scale-ups | Believed demand / backlog at the 2nd decision | Asked / formula | Full size after | Over the latency limit | Peak backlog |
|---|---|---|---|---|---|---|
| Stock, default | 2.1 | 68% / 11% | 7.6 / 8.0 | 183 s | 408 s | 210k |
| Stock, broker | 2.0 | 100% / 101% | 8.0 / 8.0 | 139 s | 266 s | 111k |
| Target, default | 3.0 | 44% / 6% | 5.1 / 8.0 | 242 s | 460 s | 240k |
| Target, broker | 2.0 | 100% / 100% | 8.0 / 8.0 | 134 s | 268 s | 119k |

- Time over the limit: -141 s [-207, -76] with the shipped band, -192 s [-235, -149] with the band at the
  target (95% t intervals). Lower in 8 of 8 runs for both (exact p = 0.031 after correction, the
  smallest value that eight pairs and four tests allow).
- With the default signal the rule takes the job's own throughput for the demand, so each step about
  doubles the parallelism: 1, 2, 5, 8 in 5 of 8 runs with the band at the target, three or more steps in 7
  of 8. With the shipped band it takes two steps (1, 3, 8) in 7 of 8 runs, later and smaller than those of the rule with the fix.
- Cost: the fixed jobs hold their full size for longer. Mean slots over the run rise from 5.7 to 6.2 and
  from 5.4 to 6.2.
- The fix has costs: more slots, and the true backlog makes the catch-up term ask for 13.6 to 14.1
  subtasks where 6 keep up; only the maximum of 8 kept the job from being oversized.

Data: `results-e8b/`. Table and figure: `paper/gen/tab_e8b.tex`, `paper/figs/e8b_surge.pdf`.
Reproduce: `bench/campaign.sh e8b`, then `python3 bench/paper.py e8b`.

### Smaller overloads (E8d, 12 runs)

In seconds the gain was larger in the larger overloads (separate campaigns; the plan has no test across
sizes). All differences are broker-side signal minus default signal.

| Overload | Stock band | Band at the target |
|---|---|---|
| 200 to 600 records/s (4 runs) | -22 s [-39, -4] | -8 s [-33, +17] |
| 200 to 1,200 records/s (8 runs) | -100 s [-128, -72] | -46 s [-64, -27] |
| 200 to 2,400 records/s (8 runs) | -141 s [-207, -76] | -192 s [-235, -149] |

- To 1,200 records/s, shipped band: the rule with the fix acts after 25 s instead of 51 s and one step is enough
  in 7 of 8 runs. It holds no more slots (3.3 against 3.7).
- To 1,200 records/s, band at the target: the rule with the fix goes to 8 subtasks in 6 of 8 runs where 3 keep
  up, and holds 5.3 slots against 4.4. This cost was predicted before the runs.

Data: `results-e8d/`. Reproduce: `bench/campaign_e8d.sh`, then `python3 bench/paper.py e8d` and
`python3 bench/paper.py signal`.

### Longer windows (E8c, 6 runs; run twice, the second execution is reported)

The same overload to 2,400 records/s for 40 minutes with a 5-minute metric window, a 2-minute
stabilization and a 5-minute catch-up time. The rule cannot decide for about 435 s after a change.

| Band | Over the limit, default signal | Broker-side signal | Difference [95% interval] | First step after (default / broker) | Full size after (default / broker) |
|---|---|---|---|---|---|
| Stock | 1,475 s | 1,098 s | -377 s [-437, -318], lower in 6 of 6 | 186 s / 29 s | 624 s / 480 s |
| Target | 1,275 s | 1,086 s | -189 s [-231, -148], lower in 6 of 6 (the planned test, p = 0.031) | 101 s / 24 s | 537 s / 474 s |

- All 24 jobs took the same two steps, 1 to 2 and 2 to 8. The stale signal did not add a step. It
  delayed the first one, and everything after it moved by the same amount.
- Two of the three predictions for this campaign failed, in both executions: we expected three or more
  scale-ups with the default signal, and full size at least one decision interval later. The third held
  (fewer seconds over the limit in every pair).
- As a fraction of the metric window, the default signal's first step came after 0.62 (stock) and 0.34
  (target) of it; with one-minute windows the fractions were 0.72 and 0.43.
- Not planned: with these windows the band at the target was better than the shipped band on the default
  signal (-200 s [-275, -126]); with one-minute windows it was not (+53 s [-4, +109]).
- Why it was run twice: in run 2 of the first execution three jobs lost a task inside the overload
  when the VM's clock stepped backwards (ISSUES.md #86). By the rule fixed before the repeat, the
  campaign was repeated in full and the repeat is the result. The repeat had no task failure, no
  rejected run and no killed container. The four predictions written for it all held.
- The first execution next to it: -413 s [-498, -328] (stock) and -247 s [-335, -158] (target), lower
  in 6 of 6 each; without its run 2, -403 s [-511, -296] and -217 s [-276, -158]. Same two steps in
  all 24 jobs.

Data: `results-e8c/` (second execution), `results-e8c-first/` (first execution). Reproduce: `bench/campaign_e8c.sh`, then `python3 bench/paper.py e8c` and
`python3 bench/paper.py signal`.

## 3. In short scenarios the broker-side signal changes little (E8, 24 runs)

Spike (2 minutes), ramp (4 minutes) and slow-down (3 minutes), band at the target, 8 runs each.

| Scenario | Over the limit, default signal | Broker-side signal | Difference [95% interval] |
|---|---|---|---|
| Spike | 165 s | 147 s | -18 s [-26, -11], lower in 8 of 8 |
| Ramp | 47 s | 44 s | -3 s [-26, +20] |
| Slow-down | 12 s | 12 s | 0 s [-9, +9] |

- The planned test pools the three scenarios: -6% of a scenario's mean [-23, +10], p = 0.89 after
  correction. No detectable effect.
- In the ramp and the slow-down the rule reacts to the rates before a backlog builds. In the spike the
  first scale-up came after 18 s instead of 28 s and asked for 4.2 subtasks instead of 3.6.
- A 4 KiB fetch size (no code) gave the rule 69% of the backlog and 99% of the demand, and the same
  outcomes as the broker-side signal.
- With its shipped band the rule used 23% fewer slots [-29, -17] than with the band at the target and
  was not over the limit for longer (-11% [-26, +4]). The planned test of the band is E3.

Data: `results-e8/`. Numbers: `paper/gen/summary_e8.txt`; the table over all closed-loop runs on the
signal is `paper/gen/tab_signal_closed.tex`.
Reproduce: `bench/campaign.sh e8`, then `python3 bench/paper.py e8` and `python3 bench/paper.py signal`.

### The same with the shipped band, and smaller fetch sizes (E8s, 30 runs)

E8 ran the fix with the band at the target only. E8s runs four jobs with the shipped band: default
signal, broker-side signal, a 64 KiB fetch and a 4 KiB fetch. 8 runs each of the spike, the ramp and the
slow-down, and 6 of the eight-minute overload of section 2.

| Scenario | Over the limit, default signal | Broker-side signal | Difference [95% interval] |
|---|---|---|---|
| Spike | 162 s | 140 s | -22 s [-32, -11], lower in 8 of 8 |
| Ramp | 56 s | 47 s | -9 s [-54, +36] |
| Slow-down | 0 s | 0 s | no job rescaled |
| Overload to 2,400 records/s | 390 s | 254 s | -137 s [-212, -62], lower in 6 of 6 |

- The planned test pools the three short scenarios: -11% of a scenario's mean [-35, +14], p = 1.0 after
  correction; scale-ups +6% [-8, +20]. No detectable effect, as with the band at the target. The fix held
  11% more slots [+5, +17].
- In the spike the first scale-up came after 22 s instead of 52 s. With the shipped band the rule acts
  only at saturation, and a true backlog counts towards the band at once.
- The overload repeats section 2 with other runs: -137 s against -141 s there.
- A 64 KiB fetch showed the rule 7% of the backlog in the spike (8 runs) and 79% in the overload (6 runs);
  the 38% that this line had before is the median over both, and 86% of the demand (default 6% and 81%).
  In the overload it changed the time over the limit by -28 s [-69, +14], lower in 4 of 6 runs. We had
  predicted at least 50 s in at least 5 of 6. The rule saw most of the backlog there, but its first step
  came as late and was as small as with the default: 2.7 subtasks after 33 s (default 2.7 after 40 s),
  against 4.2 after 18 s with the broker-side signal and 5.3 after 24 s with 4 KiB. In the spike it gave
  -23 s [-33, -13], lower in 8 of 8.
- A 4 KiB fetch showed the rule 71% of the backlog and 101% of the demand. In the overload the job was
  over the limit for 213 s, 41 s less than with the broker-side signal (we had predicted within 40 s);
  in the spike -33 s [-46, -20].
- Of seven predictions four held (the pooled test, the spike, the overload with the fix, no rescale in
  the slow-down without the fix) and three failed (64 KiB in the overload, 4 KiB against the fix by one
  second, and restarts in the slow-down with the fix, which did not happen).

Data: `results-e8s/`. Numbers: `paper/gen/summary_e8s.txt`. Reproduce: `bench/campaign_e8s.sh`, then
`python3 bench/paper.py e8s` and `python3 bench/paper.py signal`.

## 4. How much two identical jobs differ (E2, 22 runs; E2b, 5 runs)

- Four identical jobs side by side differ by 10.1% of the time over the limit and 4.1% of the slots, on
  average over 56 pairs in 16 runs. This is the margin below which the paper does not call two
  controllers different. An earlier figure of 20.4% used a different divisor than the planned contrast
  (ISSUES.md #62).
- The plugin in observe-only mode did not change the built-in rule: +1.7% [-8.7, +12.1] of the time
  over the limit, -3.0% [-9.0, +3.0] of the slots.
- The runs with one job alone are being repeated: a JobManager was killed in 5 of 6 (ISSUES.md #61).
- In the sustained overload (E2b: four identical policy-in-code jobs, 5 runs) the jobs differ by 7.1% of
  the time over the limit (27 s) and 2.5% of the slots, over 30 pairs. We had expected more than in the
  short scenarios. By run the figure is 1.6 to 19.1%; one job of 20 took three steps (17 took two, and 2
  that started at 2 subtasks took one) and was 137 s longer over the limit than the fastest of its run.
- The level of a whole run moves much more than that: 290 to 427 s over the limit in four runs, 540 to
  574 s in the fifth. All comparisons in the paper are within runs for this reason.

Data: `results-e2/`, `results-e2b/`. Reproduce: `bench/campaign.sh e2` and `bench/campaign_e2b.sh`, then
`python3 bench/paper.py e2` and `python3 bench/paper.py e2b`.

## 5. In the sustained overload the models add nothing to the policy in code, and the policy's gain is a gain over the stale signal (E10, 24 runs)

Four controllers side by side in the overload to 2,400 records/s: the built-in rule with its band at
the target (Builtin), the plugin with the policy in code and no model (PaC), with the decision model
(DM) and with the LLM. 12 runs with the default signal, 12 with backlog and arrival rate from the
broker for every controller. Time over the 2 s latency limit, mean per run:

| | Default signal | Broker-side signal |
|---|---|---|
| Builtin | 430 s, 2.8 steps, full parallelism after 235 s | 268 s, 2.0 steps, 139 s |
| PaC | 341 s, 2.0 steps, 137 s | 285 s, 2.0 steps, 137 s |
| DM | 332 s, 2.0 steps, 134 s | 273 s, 2.0 steps, 135 s |
| LLM | 343 s, 2.0 steps, 147 s | 283 s, 2.0 steps, 146 s |

- Default signal: PaC is over the limit for 89 s less than Builtin [-111, -66], in 12 of 12 runs
  (Holm-adjusted p 0.003), for 0.27 slots more. Builtin takes three steps in 10 of 12 runs, PaC two in
  every run. In 23 of PaC's 24 scale-ups its projection had doubled the believed demand, which is its
  cap. That makes up for part of what the stale signal hides.
- Broker-side signal: Builtin takes two steps in every run and PaC is not better: +16 s [-18, +51].
- Effect of the signal between the arms (separate runs): -162 s [-195, -128] for Builtin, -57 s
  [-86, -27] for PaC.
- DM minus PaC: -9 s [-33, +15] and -11 s [-44, +21]. LLM minus PaC: +2 s [-20, +23] and -2 s
  [-17, +13]. All inside the bound of plus or minus 70 s that was fixed before the runs. Two identical
  PaC jobs differ by 4 s [-23, +31], 28 s [-28, +84] and -4 s [-37, +29] (E2b).
- Against the 10% margin the LLM is not distinguishable from PaC in both arms. The DM's interval is
  inside with the default signal (-9.8 to +4.5%) and reaches -15.7% with the broker-side signal, so
  there the wording is "no detectable difference".
- Decisions: for Aggregate the DM chose the policy's option in 242 of 242 decisions and the LLM in 235
  of 235. For Measure the DM preferred one subtask fewer in 20 of 542 (all stopped by the threshold for
  scaling down). The LLM differed in 7 of 512, always towards more subtasks (applied): five scale-downs
  it did not make, one step from 1 to 2 where the policy held, one scale-up to 3 where the policy
  gives 2. No model call failed (692 and 639 calls; 0.13 s and 2.6 s on average).
- The situation the LLM's prompt gives it latitude for (busy 95% or more, believed demand below
  capacity) came up for Aggregate in 2 cycles in which a larger size was on offer. The policy scaled up
  in both, to the maximum of 8, so the LLM could not go above it. (It also holds in 51 and 45 cycles in which Aggregate ran
  at the maximum and no model is asked: a job that drains a backlog looks the same.) For Measure, which the plan's count leaves out, it came up once, with sizes above the policy's on offer, and there the LLM
  chose 3 subtasks where the policy gives 2. So it had one chance to go above the policy and took it,
  and it was not on the bottleneck.
- The LLM's job reaches full parallelism 9 to 10 s later than PaC's. The time over the limit does not
  show it.
- The plugin's controllers rescale more often (4.8 against 3.1 per run with the default signal, 3.3
  against 2.6 with the broker-side signal).
- One run of the default arm was made again (the original is in `results-e10-first/`): three of its
  jobs had lost a task to the clock fault (ISSUES.md #86) inside the overload and all four stayed over
  the limit to the end of the run. The rule for this was written before any outcome was read. With the
  original in place of the replacement: PaC minus Builtin -79 s [-106, -52], DM minus PaC -8 s
  [-32, +16], LLM minus PaC -2 s [-22, +17]. No verdict differs.

Data: `results-e10/`. Reproduce: `bench/campaign_e10.sh`, then `python3 bench/paper.py e10`.

## 6. Six load shapes of other studies: the models follow the policy, and the built-in rule is late less on rate levels in one random order (campaign lit, 24 runs, exploratory)

Six shapes that other autoscaling studies use, at this testbed's rates, each twice with the default fetch
size and twice with a 4 KiB fetch (the night of 19 to 20 Nov 2026 and the early morning of 22 Nov). No prediction was registered.
One run per cell and repetition; both values are shown, no mean.
Numbers: `paper/gen/lit_default.csv`, `paper/gen/lit_accurate.csv` (`bench/lit_arm.py`).

Seconds over the 2 s limit, repetition 1 / repetition 2, default fetch size:

| Shape | Min | Built-in | Policy in code | Decision model | LLM |
|---|---|---|---|---|---|
| Idle, then burst (replay) | 10 | 197 / 299 | 240 / 228 | 239 / 238 | 237 / 232 |
| Constant rate (steady) | 20 | 15 / 0 | 15 / 0 | 28 / 0 | 25 / 0 |
| Square wave (onoff) | 24 | 166 / 159 | 91 / 111 | 157 / 138 | 165 / 173 |
| Square wave, first run (set aside, ISSUES #96) | 24 | 153 | 129 | 142 | 222 |
| Random walk (walk1) | 25 | 7 / 7 | 15 / 7 | 4 / 6 | 14 / 5 |
| Cosine (cycle) | 30 | 68 / 69 | 58 / 59 | 67 / 61 | 62 / 56 |
| Rate levels in one random order, the same in all runs (levels1) | 32 | 18 / 6 | 104 / 66 | 104 / 66 | 80 / 136 |

4 KiB fetch:

| Shape | Built-in | Policy in code | Decision model | LLM |
|---|---|---|---|---|
| Idle, then burst | 191 / 284 | 202 / 203 | 212 / 207 | 204 / 223 |
| Constant rate | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Square wave | 107 / 93 | 182 / 95 | 118 / 104 | 101 / 171 |
| Random walk | 12 / 12 | 7 / 17 | 5 / 3 | 6 / 13 |
| Cosine | 65 / 84 | 52 / 46 | 33 / 61 | 61 / 64 |
| Rate levels in one random order | 19 / 22 | 98 / 77 | 56 / 71 | 87 / 131 |

Mean slots, rescalings and largest backlog per run are in the two CSV files.

- The models chose what the policy chose on the same inputs. In the measured phases, by the definition of
  18 Nov: decision model 2,769 of 2,783 decisions (1,385 of 1,392 and 1,384 of 1,391 in the two
  repetitions), LLM 2,399 of 2,402 (1,161 of 1,163 and 1,238 of 1,239). No failed call, no fallback.
- Rate levels: the built-in rule's lead repeats in all four runs, 6 to 22 s against 56 to 136 s. After
  the drop from 2,400 to 300 records/s the built-in rule kept 4 Aggregate subtasks in three runs and all
  8 in the fourth, which carry the next level of 1,800 records/s; the three others went down to 2 or 3
  in every run, and most of their late seconds fall into the four minutes after the jump (59 to 85 s
  of their 80 to 104 s with the default fetch size in repetition 1, counted over seconds 534 to 773). Over the run the built-in
  rule changed the size it asked for 1 to 3 times, the others 6 or 7 times (changes of the target size
  in the samples of the measured phase), and it held 7.2 to 7.5 slots against 6.4 to 6.7.
- Idle then burst: does not repeat. The built-in rule was ahead in repetition 1 (by 11 to 43 s) and
  behind in repetition 2 (by 61 to 81 s), with both fetch sizes. The first write-up said this shape went
  the same way as the rate levels with a smaller gap; that sentence is withdrawn.
- Square wave: one controller's two runs differ by up to 87 s (policy, 4 KiB fetch: 182 and 95 s), so no
  ordering of the controllers holds in all four runs.
- Cosine: the built-in rule has the highest value in all four runs, by 1 to 20 s over the next.
- Not compared: the two arms with each other (separate nights in repetition 1).

## 7. On a shared EKS cluster with managed Kafka the models again follow the policy (cluster test, scale 2, 11 runs, exploratory)

The same four controllers side by side on a shared non-production EKS cluster instead of the laptop (night of
2026-11-20): Flink 2.3, operator 1.16.1 with the plugin in its own Deployment, Kafka on a managed cluster (MSK,
IAM login), checkpoints on S3, metrics over OpenTelemetry to Datadog. "Scale 2" doubles the laptop's rates,
partitions (16), key groups (240), largest parallelism (16) and source parallelism (4). Three runs per short
scenario and two runs of the rate levels. No prediction was registered, and two or three runs per cell do not
rank controllers.

Two notes added on 2026-11-21 after review round 2, which hold for sections 7 to 10:
- Counts of model against policy in these sections are those of the cluster scripts: the applied
  target against the policy over the whole log, warm-up and vertices with one option included. By the
  definition the plan fixed for the laptop campaigns (measured phase, model asked about the vertex, most
  likely option; `python3 cluster/bench/decount.py <run>...`) they are: four controllers 422 of 427
  (decision model; five below the policy, none applied) and 342 of 343 (LLM); announced spike, LLM without
  a note 114 of 114; unrelated note 54 of 54; rate levels 523 of 526 and, with history, 384 of 776
  different; on-off 256 of 256 and, with history, 43 of 251 different (after the connector upgrade 271 of
  275 and 69 of 317). No statement of these sections changes. The paper uses the plan's definition.
- Three restarts of this first campaign lost a TaskManager that the job needed, as in ISSUES.md #105; no
  logs were kept, and the pod counts show them (`python3 cluster/bench/tmdrop.py results-cluster/s2`): rate
  levels run 1, the policy's job at 611 s; rate levels run 2, the built-in job at 591 s; ramp run 2, the
  decision model's job at 326 s. Each ran with 12 instead of 16 subtasks for 25 to 30 s. The 147 s of the
  policy and the 44 s of the built-in rule on the rate levels below are these two runs.

Seconds over the 2 s limit, mean of three runs (the three runs) / mean slots, from Datadog:

| Scenario | Built-in | Policy in code | Decision model | LLM |
|---|---|---|---|---|
| Spike, 800 to 4,000 records/s for 2 min | 172 (163, 190, 162) / 9.8 | 159 (173, 137, 168) / 10.3 | 158 (165, 143, 166) / 10.4 | 164 (143, 166, 182) / 10.2 |
| Ramp, 600 to 3,600 records/s over 4 min | 124 (108, 220, 44) / 7.1 | 63 (37, 95, 57) / 9.3 | 73 (97, 62, 61) / 9.2 | 69 (102, 65, 39) / 9.0 |
| Scale-down, 3,600 to 600 records/s | 1 (1, 1, 1) / 15.0 | 3 (3, 3, 4) / 14.0 | 3 (4, 4, 2) / 13.1 | 4 (5, 3, 3) / 13.6 |
| Rate levels, 8 levels of 4 min (levels1), 2 runs | 37 (29, 44) / 14.2 | 86 (147, 24) / 14.2 | 80 (136, 23) / 14.2 | 187 (152, 222) / 13.8 |

The harness's own files for the same runs give 176, 165, 163, 170 s (spike), 129, 68, 79, 74 s (ramp),
1, 4, 4, 3 s (scale-down) and 41, 89, 82, 190 s (rate levels), in the same order.

- All 44 job-runs are valid and have latency data to the end. No broker stall, no failed run.
- The models chose what the policy chose: decision model 658 of 658 decisions, LLM 533 of 534 (1 above).
  No failed call, no fallback. In the nine short runs the median call took 55 ms (95th percentile 92 ms)
  for the decision model and 2.5 s (3.9 s, longest 5.2 s) for the LLM.
- The one LLM decision above the policy: 19 s after the last level change (1,200 to 1,800 records/s),
  Aggregate at 6 subtasks, policy 8, LLM 10. After that it followed the policy again: hold at 10 for two
  cycles, then down to 8 (158 s after its choice). One decision in a run of 32 minutes.
- Spike: the policy and both models decided the scale-up 11 to 19 s after the spike began and had it in
  effect after about 50 s; the built-in rule had it in effect after about 80 s. The time over the limit is
  about the same for all four, and 107 to 111 s of it fall into the two minutes of the spike itself.
- Ramp: the built-in rule was late longer in two of the three runs and used about two slots less. In the
  third run it was not, and in the first the margin to both models is 6 to 11 s. One run of the built-in
  rule (220 s) carries most of the difference in the means.
- Scale-down: the built-in rule did not change its size within the run (the load drops 60 to 120 s into a
  run of 360 s, and it delays a scale-down by 2 minutes). The three others scaled down twice.
- Rate levels (4,800, 600, 3,600, 3,000, 4,200, 2,400, 1,200, 1,800 records/s): where a job was late for
  more than a minute, the late seconds fall into the third level, after the jump from 600 to 3,600
  records/s, and they follow the size of Aggregate at the jump. The built-in rule stood at 8 subtasks in
  both runs and was late 23 and 46 s there (harness). The policy and both models stood at 5, 5 and 6 in the first run (133, 122, 137 s late). In the
  second run the policy and the decision model stood at 8 (6 and 7 s) and the LLM at 4 (198 s), with the
  same choices as the policy in every decision before the jump. So the spread between jobs here is
  where a scale-down happened to stand when the load came back, not a model's judgement. Over a run the
  built-in rule changed Aggregate 4 or 5 times, the others 9 to 11 times, and 3 to 5 of their scale-downs
  were followed by a scale-up within five minutes (built-in: 1 or 2). Same direction as on the laptop
  (section 6).
- Datadog has up to 15 measured seconds less per job-run than the harness in the short runs and 15 to 36 s
  less in the long runs, and up to 9 s less over the limit; the difference grows with the number of
  restarts (ISSUES.md #104). Compare within one source.
- The two sources also count slots differently. The harness counts the parallelism the job runs with.
  Datadog counts the slots the job holds, and after a scale-down a job holds the freed slots for about
  100 s more (93 to 131 s in the runs of section 8) before its TaskManager pods are stopped. So Datadog's
  slot means are higher, by 0.3 to 1.4 slots in the means of section 8. Datadog's number is the one that is
  paid for.

A first campaign at scale 4 (8 runs, same night) is not reported as a result. A broker of the Kafka cluster
stopped answering for a minute or more in several runs: one run failed, three job-runs lost their latency
data and in one run all four jobs were tens of seconds late (ISSUES.md #103, #104). In its decisions the
decision model differed from the policy in 2 of 292 and the LLM in 1 of 250. That one is the only time a
vertex was blocked from outside: the latency sink of the LLM's job waited on the stalled broker, so its
vertex took no input and showed 92% busy time. The policy's choice was 3 subtasks (no demand), the LLM chose
20 (busy). Neither helps a blocked sink. One observation, in a run that is not counted.

Data: `results-cluster/s2/` (harness files; they hold names of the cluster and are not published),
`results-cluster/s2/datadog-main.json`. Queries: per run window (`t0_utc` to `end_utc` of `meta.json`),
`sum:flink.taskmanager.job.task.operator.secondsOverLimit{... AND subtask_index:0} by {service.name}.as_count()`
summed; slots as the mean of `max:flink.jobmanager.taskSlotsTotal` minus `taskSlotsAvailable`.
Harness view: `python3 cluster/bench/expsum.py results-cluster/s2/main`.

## 8. Told about a spike in advance, a schedule entry in code and the LLM with a free-text note are both ready for it (cluster test, scale 2, 6 runs)

Sections 5 to 7 say that a model adds nothing to the policy on the policy's own inputs. This experiment
gives a controller something the inputs do not hold: that a spike is coming. Same cluster and setup as
section 7, night of 2026-11-20, plugin with the two new inputs (operator image `cluster5`). Five jobs side by
side on the same load, a spike from 800 to 4,000 records/s for 2 minutes, whose start is fixed on a whole
minute before the jobs start. No prediction was registered.

- built-in rule, policy in code, LLM (Claude): told nothing, as before;
- policy with a schedule entry: start, end and rate of the spike as three numbers in the job's
  configuration (`job.autoscaler.jev.schedule`). From 120 s before the start (restart time plus the gap
  between two decisions) until the end, the policy sizes for the larger of the measured and the planned rate;
- LLM with a note: the job's owner's words in the job's configuration, passed to the model as they are:
  "Heads-up from marketing: the push notification for the sale goes out at 05:56 UTC today (21 November
  2026). Last time the input rate jumped from about 800 to about 4,000 records per second within a few
  seconds, stayed there for about 2 minutes and then dropped back." The prompt of this job says what a
  note is and to make the change when the announced start is less than 120 s away.

Seconds over the 2 s limit, mean of six runs (the six runs), and mean slots:

| Job | Over the limit, Datadog | Slots, Datadog | Over the limit, harness | Aggregate subtasks when the spike began |
|---|---|---|---|---|
| Built-in | 161 (154, 164, 156, 168, 159, 164) | 9.3 | 167 | 4 |
| Policy in code | 157 (142, 155, 141, 180, 141, 182) | 10.2 | 162 | 4 |
| Policy with schedule entry | 13 (24, 14, 12, 11, 8, 9) | 14.0 | 15 | 15 |
| LLM | 164 (189, 149, 139, 141, 168, 196) | 9.9 | 169 | 4 |
| LLM with note | 9 (7, 9, 13, 8, 7, 9) | 14.1 | 11 | 15 (16 in one run) |

- All 30 job-runs are valid and have latency data to the end. No broker stall, no failed model call, no
  fallback.
- In every run both informed jobs were over the limit for 7 to 24 s and the three others for 139 to 196 s.
  The smallest gap between an informed and an uninformed job within a run is 118 s.
- Both informed jobs did the same thing. The policy with the schedule entry decided the scale-up of
  Aggregate from 4 to 15 subtasks 105 to 118 s before the spike and had it in effect 70 to 82 s before.
  The LLM with the note decided it 108 to 114 s before (4 to 15, once 4 to 16) and had it in effect 70 to
  81 s before. Both decided the step to 16 within 33 s after the spike began and the scale-down 35 to
  63 s after it ended (the policy with the schedule entry 144 s after, in one run). The policy and the LLM
  that were told nothing decided their first scale-up 8 to 27 s after the spike began; the built-in rule
  had its first in effect after 60 to 89 s.
- Where the late seconds are (harness, means): the uninformed jobs were late for 110 to 112 of the spike's
  120 s and for 52 to 59 s after it. The informed jobs were late for 2 to 3 s in the spike, 2 to 7 s after
  it, and 6 to 7 s before it. Those seconds before the spike are the restart of their own early scale-up:
  being ready costs a restart too, but at 800 records/s and not at 4,000.
- The cost is 4 to 5 slots more on average over the 7-minute run (14.0 and 14.1 against 9.3 to 10.2).
  The informed jobs run with 15 to 16 subtasks from about 75 s before the spike to about a minute after
  it. The policy and the LLM that were told nothing decide the step to 16 only 10 to 32 s after the spike
  has ended and stay there for 150 to 160 s.
- The LLM without a note chose what the policy chose in 312 of 312 decisions. The LLM with the note
  differed in 20 of 266: the early scale-up (Aggregate and the sink, 12 decisions in 6 runs), 7 times one
  or two subtasks more than the policy in the first decision after the spike began, and once one subtask
  less for the sink. Its calls took longer: median 4.6 s (longest 12.2 s) against 2.4 s (longest 4.0 s),
  with about 560 more input tokens and twice the output tokens. The plugin's limit was 8 s per attempt
  with one retry, so the 12.2 s are most likely a second attempt (ISSUES.md #106).
- The two informed jobs do not differ beyond one event. In run 1 three of the four TaskManager pods of the
  policy-with-schedule job were stopped one second after its scale-down from 16 to 8 was applied; the job
  ran at 4 subtasks for 35 s until a new pod came and was late for 20 s after the spike (2 to 7 s in its
  other runs). This happened in 1 of the 100 scale-downs that the plugin applied in the cluster test up
  to then. The cause was found in the later runs and is not in the controller: the job's Kafka sink throws
  when it is closed during the cancel, and Flink stops the TaskManager for it (ISSUES.md #105, section 10).

What this shows and what it does not:

- The gain comes from the information and not from the model. A rule that is given the spike as three
  numbers does as well as the LLM that is given it as a sentence.
- What the LLM adds is the form of the input. The schedule entry needs someone to turn "the push goes out
  at 05:56" into two epoch seconds and a rate. The LLM took the time, the date and the rate out of the
  owner's sentence in 6 of 6 runs and acted in the first decision inside the lead time.
- The prompt tells the LLM what to do with a note and how long before the start to act. The LLM did not
  find that out by itself.
- One spike shape, one wording, a note that is right. Other wordings (another time zone on a 12-hour
  clock, "5 times its normal level" instead of rates, a note with the same time that says nothing about
  load) are in section 9. A spike that is announced and does not come is in the subsection below.

Data: `results-cluster/announced/` (harness files, not published), `results-cluster/announced/datadog.json`.
Harness view: `python3 cluster/bench/expsum.py results-cluster/announced`. Queue: `cluster/bench/queue_announced.sh`.

### A false alarm: the announced spike does not come (3 runs, 2026-11-21, 16:05Z to 16:52Z)

The same runs with one change: the load stays at 800 records/s, while the schedule entry and the note
still announce the spike to 4,000 records/s (`--no-event`). Four jobs: the policy and the LLM, each
without and with the announcement. Same images and settings as above.

| Job | Over the limit, Datadog | Slots, Datadog | Restarts | Aggregate, seconds from the announced start |
|---|---|---|---|---|
| Policy in code | 0 (0, 0, 0) | 4.0 | 0 | no change |
| Policy with schedule entry | 9 (10, 9, 7) | 13.8 | 3 to 4 | 4 to 15 at -75 to -54 s, 16 at +20 to +51 s, 8 at +175 to +201 s |
| LLM | 0 (0, 0, 0) | 4.0 | 0 | no change |
| LLM with note | 10 (11, 10, 10) | 14.5 | 3 | 4 to 15 at -71 to -59 s, 16 at +39 to +50 s, 8 at +189 to +211 s |

- Both informed jobs scaled up for the spike that did not come, in 3 of 3 runs, at the same lead as in
  the six runs with a spike. They held 15 to 16 Aggregate subtasks through the announced two minutes and
  went to 8 about a minute after the announced end (55 to 91 s), when the cooldown of their last change
  had passed. The policy with the schedule entry reached 4 again in two of three runs before the run
  ended; the LLM with the note was still at 8.
- A false alarm cost what a true one costs and bought nothing: about 14 slots against 4 over the seven
  minutes of the run, 3 to 4 restarts, and 7 to 11 s over the latency limit in the restarts, where the
  uninformed jobs had none.
- Neither controller checked the announcement against the load. The LLM saw 745 to 851 records/s in
  every decision. Inside the announced window it added capacity once more in all three runs (Aggregate
  15 to 16 and the sink 2 to 4 or 5, where the policy on the same inputs held), and it went down when
  the policy did, after the announced end. The note says that the spike lasts about two minutes; nothing in the prompt says
  what to do when it does not come.
- Three runs, one kind of wrong note. A spike that comes later than announced was not tested; by the
  timing above it would meet a job that has just scaled down.

Three models on the same note (3 more runs, 16:59Z to 17:46Z). The job with the schedule entry and
three LLM jobs with the note side by side: Claude Haiku 5.5, which all other runs use, Sonnet 5.5 and
Opus 5.5. The question was whether a larger model goes back when the announced spike does not come.

| Job | Over the limit, Datadog | Slots, Datadog | Aggregate, seconds from the announced start |
|---|---|---|---|
| Policy with schedule entry | 10 (8, 10, 11) | 13.8 | 15 at -72 to -67 s, 8 at +183 to +188 s |
| Haiku with note | 7 (5, 11, 6) | 14.4 | 15 or 16 at -73 to -62 s, 8 at +183 to +213 s |
| Sonnet with note | 9 (10, 11, 6) | 13.2 | 12 or 15 at -73 to -22 s, 16 at +48 to +108 s, 8 at +187 to +243 s |
| Opus with note | 10 (10, 9, 10) | 14.2 | 16 at -68 to -57 s, 8 at +193 to +208 s |

- No model went back early. All three scaled up before the announced start in 3 of 3 runs and scaled
  down at the first decision at which the policy on the same inputs did, 177 to 233 s after the
  announced start.
- A model has few decisions in which it could go back, because every change is followed by about 105 s
  without a decision. Each job decided once inside the announced two minutes, 15 to 73 s into them, with
  a measured rate of about 800 records/s. At that decision every model added capacity where the policy
  held, in 9 of 9 cases: the sink from 2 or 3 to between 3 and 7 subtasks, and Aggregate to 16 where it
  was at 12 or 15. Each of these is one more restart.
- Sonnet left the policy before the event in two of three runs: it took Aggregate from 4 to 3 in the
  quiet phase, where the policy holds a vertex that is oversized by one subtask. Its scale-up, which the
  plugin limits to four times the current size, then went to 12.
- Calls: median 4.3 s for Haiku, 3.4 s for Sonnet and 3.5 s for Opus with this prompt, longest 9.0, 24.7
  and 6.5 s; every call was answered.
- Three runs; the models are not compared on outcomes here, only on what they did.

Data: `results-cluster/falsealarm/`, `results-cluster/falsealarm-models/` (harness files, not published) and
the `datadog.json` in each. Harness view: `python3 cluster/bench/falsesum.py results-cluster/falsealarm/spike/rep*`.
Queue: `cluster/bench/queue_falsealarm.sh` (`FA_NAME=falsealarm-models FA_JOBS=rule+a,llm+a,llm+sonnet+a,llm+opus+a`
for the three models).

## 9. Other wordings of the note: the LLM acted on three of three that announce load and on none that does not (cluster test, scale 2, 3 runs, exploratory)

Section 8 has one wording of the note. Here the same spike (800 to 4,000 records/s for 2 minutes, start
fixed on a whole minute before the jobs start) runs with five jobs side by side: the policy in code, told
nothing, and four jobs with the LLM (Claude) and a note each. Same prompt as in section 8. Night of
2026-11-21, 09:51Z to 10:38Z, three runs. The notes of run 1 (the time is the run's own):

- first: the note of section 8. "Heads-up from marketing: the push notification for the sale goes out at
  10:00 UTC today (21 November 2026). Last time the input rate jumped from about 800 to about 4,000
  records per second within a few seconds, stayed there for about 2 minutes and then dropped back."
- time zone: the same with "goes out on Saturday, 21 November, at 2:00 am Pacific Standard Time".
- no rates: the same with "Last time the input rate went up to about 5 times its normal level".
- unrelated: "Reminder from the platform team: the dashboards of this job move to the new folder at 10:00
  UTC today (21 November 2026), and the on-call rotation changes on Monday. Nothing changes for the job
  itself."

Seconds over the 2 s limit in the three runs, and mean slots:

| Job | Over the limit, Datadog | Slots, Datadog | Over the limit, harness | Aggregate subtasks when the spike began | Scale-up decided, seconds from the spike's start |
|---|---|---|---|---|---|
| Policy in code | 161 (177, 166, 139) | 10.4 | 166 (182, 174, 142) | 4 | +5, +9, +16 |
| LLM, first note | 23 (26, 35, 8) | 12.8 | 28 (33, 40, 10) | 15 | -109, -109, -113 |
| LLM, time zone | 7 (4, 10, 8) | 14.4 | 8 (6, 10, 8) | 15 | -316, -113, -113 |
| LLM, no rates | 13 (16, 12, 12) | 14.5 | 15 (18, 13, 14) | 15 | -109, -106, -108 |
| LLM, unrelated note | 160 (142, 188, 151) | 10.1 | 167 (144, 196, 161) | 4 | +29, +10, +27 |

- All 15 job-runs are valid in the harness data. No failed model call, no fallback.
- The three notes that announce load were acted on in 9 of 9 job-runs: Aggregate stood at 15 subtasks when
  the spike began, as in section 8. The job with "5 times its normal level" chose 15 as well, so the LLM
  took the size from the measured rate and the factor.
- The unrelated note changed nothing. That job chose what the policy chose in 154 of 154 decisions, made
  its first scale-up 10 to 29 s after the spike began and was late as long as the policy (160 against
  161 s).
- With the time-zone note (2:00, 2:15 and 2:31 am Pacific Standard Time) the job was ready in all three
  runs. In runs 2 and 3 the LLM acted 113 s before the start. In run 1 it decided the scale-up (4 to 16) 316 s
  before the start, in its second decision, where the prompt says to act when the start is less than
  120 s away. It held 16, went to 15 at 40 s before the start and was ready. The cost of acting early is
  slots and nothing else here. The decision record has no text from the model, so why it acted early is
  not known. The first note never led to an early action in its nine runs (six in section 8, three here).
- The job with the first note was late for 26 and 35 s in runs 1 and 2 and for 8 s in run 3. In runs 1
  and 2 three of its four TaskManagers stopped at its scale-down after the spike (ISSUES.md #105, the
  Kafka sink's close error). It ran with 4 subtasks for 35 s and was late for 21 and 23 s after the spike,
  against 0 to 4 s for the two other informed jobs. Its decisions were the same as theirs. The same error
  hit the job with the unrelated note twice (one TaskManager each: at a scale-down 9 s before the end of
  run 1, and at its scale-up in the spike of run 3, which reached the decided size 20 s later).
- Calls with the time-zone note took longer: median 5.0 s (longest 7.9 s) and 947 output tokens on
  average, against 2.6 s (9.2 s) and 598 for the first note, 2.9 s (9.3 s) and 595 without rates, and
  2.4 s (3.7 s) and 409 for the unrelated note.
- For the job with the first note in run 1 Datadog has 377 of 420 seconds, just under the 90% that we ask
  for. The harness has its latency to the end. The missing seconds are the counters of the three
  TaskManagers that stopped (ISSUES.md #104).

What this shows and what it does not:

- The LLM's use of a note does not hang on one wording: another time zone on a 12-hour clock and a factor
  in place of rates gave the same early scale-up, and a note with the same time in it that is not about
  load gave none.
- Three runs, one spike shape, notes that are right. One early action in three runs with the time-zone
  wording is an observation, not a rate. A rule in code cannot be early in this way, and it cannot read
  any of the four notes.

Data: `results-cluster/notes/` (harness files, not published), `results-cluster/notes/datadog.json`.
Harness view: `python3 cluster/bench/expsum.py results-cluster/notes`. Queue: `cluster/bench/queue_notes.sh`.

## 10. A load that falls and comes back within minutes: the plugin's policy runs out of phase with it, looking back 10 minutes holds capacity in code and in the prompt alike, and the built-in rule's scale-down delay costs the least (cluster test, scale 2, 5 and 5 runs and 5 more on-off runs, exploratory)

A scale-down that is undone minutes later is a known complaint about the built-in autoscaler. The idea
tested here: give a controller the job's own past, so that it does not scale down when the load is likely
to return. Same cluster and setup as sections 7 to 9, night of 2026-11-21, 07:05Z to 11:37Z. Five jobs
side by side on the same load:

- built-in rule, as in section 7 (upper bound of the band at the target, `scale-down.interval` 2 minutes);
- policy in code;
- policy with a look-back (`job.autoscaler.jev.scale-down.window-seconds: 600`): when it scales a vertex
  down, it sizes it for the highest demand of the last 10 minutes;
- LLM (Claude);
- LLM with history (`job.autoscaler.jev.llm.history`): the prompt also holds the source's rate over the
  last half hour and, for each earlier change of the job, what it cost (how long the job gave no
  measurements, the backlog before, its peak after, how long it took to clear, and the next change of the
  same vertex). The instructions say: "If the demand has been going up and down, or an earlier scale-down
  was followed by a scale-up within a few minutes, a scale-down now is likely to be undone soon and to cost
  two restarts. In that case size the vertex for the highest demand of the last 10 minutes instead of the
  current demand [...], and keep the current parallelism unless it has 2 or more subtasks more than that."

Two loads: on-off (800 and 3,200 records/s, 2 minutes each, six times, 24 minutes, five runs) and rate
levels (4,800, 600, 3,600, 3,000, 4,200, 2,400, 1,200 and 1,800 records/s, 4 minutes each, 32 minutes, five
runs: two in that night and three on 2026-11-21 from 13:24Z to 15:18Z with the same images and settings).
All 50 job-runs are valid, and Datadog has at least 95.7% of each job's seconds. In on-off runs 1
and 2 the plugin's limit for a model call was 8 s, and the LLM with history ran into it 4 times and once
and got the policy's choice (ISSUES.md #106). From run 3 on the limit is 20 s and no call failed.

### On-off load

Every job starts at 4 subtasks, so the first high phase is the same unannounced jump for all of them: 105
to 171 s over the limit, for every job in every run. The comparison starts with the second high phase,
about 17 minutes per run. Means of five runs (the five runs):

| Job | Over the limit, Datadog | Over the limit, harness | Restarts | of them for the sink only | Slots of the job's pods, Datadog |
|---|---|---|---|---|---|
| Built-in | 11 (26, 3, 14, 0, 13) | 12 (29, 3, 15, 0, 13) | 2.4 (2, 2, 4, 0, 4) | 0 | 14.1 |
| Policy in code | 20 (15, 33, 19, 19, 16) | 26 (21, 37, 28, 21, 22) | 7.2 (7, 7, 7, 7, 8) | 0.8 | 15.6 |
| Policy with look-back | 18 (16, 21, 20, 21, 13) | 22 (20, 24, 25, 26, 13) | 7.4 (7, 8, 7, 8, 7) | 1.2 | 15.8 |
| LLM | 23 (45, 18, 13, 21, 17) | 28 (52, 24, 16, 27, 23) | 6.8 (8, 6, 6, 7, 7) | 1.8 | 15.5 |
| LLM with history | 12 (12, 7, 18, 15, 9) | 15 (12, 12, 21, 21, 9) | 6.2 (4, 6, 8, 7, 6) | 2.8 | 15.7 |

- The policy in code runs out of phase with this load. Of its 21 scale-downs of Aggregate, 15 came into
  effect in a high phase, ten of them 2 to 22 s after the load had come back, and 9 of its 11 scale-ups
  came into effect in a low phase, 2 to 37 s after the load had fallen. A decision, its restart and the
  wait before the next decision take about as long as one phase. The job spent 32 to 60% of the high
  phases at 8 or fewer subtasks.
- It restarted 7 to 8 times in 17 minutes and freed almost no pods: the job's TaskManager pods held 15.6
  of 16 slots on average. A pod that a scale-down frees is released 70 to 115 s later (median 90 s), and
  in 28 of the 53 scale-downs of the four plugin jobs that freed a pod, the next change came first.
- The late seconds stay small because the half size is just enough: 8 subtasks carry about 3,560
  records/s at full load and the high level is 3,200. After the first cycle the jobs are late for 2 to 5 s
  per restart. A higher high level would have cost the policy far more; this load understates the harm.
- The look-back in code keeps the capacity: that job was never at 8 or fewer subtasks in a high phase.
  It does not remove the restarts. It scales down to 12 and at each jump back to 16, because the policy's
  trend projection doubles the demand in the first decision after a jump (12 subtasks were enough, at 60%
  busy). So 7 to 8 restarts per run are left, for steps that change nothing.
- The LLM with history changed Aggregate less often, 3.4 times per run against 5.0 to 6.4 for the three
  other plugin jobs, and 8 of its 10 scale-downs came into effect in a low phase. It restarted about as
  often (6.2 against 6.8 to 7.4), because it followed the policy on the sink (4 to 2 subtasks and back),
  and a change of the sink restarts the whole job: 2.8 of its restarts per run were for the sink only.
  The instructions speak about scaling a vertex down and do not say what a restart for a small vertex costs.
- The built-in rule changed least (0 to 4 restarts), held the fewest slots (14.1) and was late as little
  as any. Its scale-down delay of 2 minutes is as long as the low phase, so most of the time the load was
  back before it could act. In run 4 it stayed at 12 subtasks from the second high phase to the end and
  was not late once. When it did scale down, 6 of 7 times the change came into effect in a high phase too.
- In late seconds the four plugin jobs are not told apart. The same job differs by up to 32 s between runs
  (LLM: 13 to 45 s), and in the three runs with the 20 s limit the four are at 14 to 18 s on average and
  the built-in at 9 s.
- The LLM chose what the policy chose in 332 of 332 decisions. The LLM with history differed in 43 of 308,
  always with more subtasks. Its calls took longer: median 3.2 to 4.3 s per run (longest 12.9 s) against
  2.3 to 2.8 s (longest 7.3 s), with about 900 more input tokens. In runs 3 and 4 the saved operator log
  begins 113 s and 57 s into the run, before the first high phase (ISSUES.md #109), so these counts lack
  the decisions of that time, about four per job.
- The sink's close error (ISSUES.md #105) stopped TaskManagers three times: for the LLM in run 1 at two
  scale-downs, and for the LLM with history in run 3 (8 instead of 12 subtasks for 30 s, 13 late seconds
  in that low phase).

### On-off load again, with the newer Kafka connector (five more runs)

The sink's close error comes from the Kafka connector the test job was built with (4.0.1; ISSUES.md #105).
The job was rebuilt with connector 5.0.0-2.2 and the on-off load was run five more times with the same
five jobs and settings, on 2026-11-21 from 17:46Z to 20:15Z (`results-cluster/fix105/flapping/onoff`). The
runs answer two questions: does the stop go away, and does the picture above hold a second time.

The stop went away. In 207 restarts no TaskManager left without a release request. The TaskManager logs
hold 533 forced closes of a producer and not one close error of the writer. With the old connector the
saved logs hold 11 restarts with a stop in 531, and 3 in 214 in the five on-off runs above. At the rate of
all saved restarts, none in 207 has a chance of 1.3%; at the on-off runs' own rate, 5%. The counts agree
with the code, where the step that kept the error is gone (`python3 cluster/bench/fixcheck.py`, ISSUES.md #105).

From the second high phase on, means of the five new runs:

| Job | Over the limit, Datadog | Over the limit, harness | Restarts | of them for the sink only | Slots of the job's pods, Datadog |
|---|---|---|---|---|---|
| Built-in | 17 (31, 7, 11, 12, 25) | 18 (34, 7, 11, 14, 25) | 3.0 (3, 3, 4, 2, 3) | 0.2 | 15.1 |
| Policy in code | 26 (24, 38, 39, 18, 13) | 31 (29, 42, 44, 22, 16) | 7.4 (7, 8, 7, 8, 7) | 1.2 | 15.8 |
| Policy with look-back | 19 (23, 18, 18, 23, 12) | 24 (32, 21, 23, 29, 13) | 7.4 (8, 8, 7, 8, 6) | 0.8 | 15.7 |
| LLM | 17 (16, 13, 11, 20, 23) | 21 (19, 16, 17, 27, 25) | 6.8 (7, 7, 6, 7, 7) | 1.4 | 15.7 |
| LLM with history | 14 (17, 5, 8, 22, 17) | 16 (17, 5, 8, 28, 20) | 5.4 (6, 6, 3, 6, 6) | 2.2 | 15.9 |

- What held: the four plugin jobs restart 6 to 8 times in 17 minutes (one run of the LLM with history
  had 3) and their pods keep 15.7 to 15.9 of 16 slots. The policy's scale-downs land in the high phase
  again: 10 of 18, nine of them 2 to 17 s after the load had come back (15 of 21 before). Its scale-ups
  land in a low phase less often than before (5 of 13, against 9 of 11). The look-back in
  code still restarts as often as the policy (7.4 and 7.4). The LLM with history again changed Aggregate
  least (3.2 times per run against 5.4 to 6.6) and again restarted for the sink (2.2 per run). The
  built-in rule again restarted least (2 to 4 times), and 6 of its 7 scale-downs came into effect in a
  high phase, as before.
- What did not hold as strongly: the built-in rule's pods held 15.1 slots, not 14.1. The earlier mean
  rested on run 4, in which it stayed at 12 subtasks. Its late seconds are 17 here, the same as the LLM's
  and above the LLM with history. Over the ten runs it is at 14 s, with the plugin jobs at 13 to 23 s,
  and holds 14.6 slots against 15.6 to 15.8.
- In late seconds the jobs are again not told apart. The policy was at 26 s and the LLM at 17 s, where
  the first five runs had them at 20 and 23 s. The same job differs by up to 26 s between runs.
- The LLM chose what the policy chose in 336 of 340 choices (3 above, 1 below). The LLM with history
  differed in 69 of 380, always with more subtasks. Calls: median 2.3 to 2.7 s per run (longest 5.9 s),
  and 3.5 to 4.2 s (longest 10.4 s) with history. No call ran into the 20 s limit.
- Datadog has 95.1 to 99.3% of each job's seconds in these windows. The numbers are in
  `results-cluster/fix105/datadog.json`.

### Why the look-back job scales up again at each rise, and what a cap has to do (replay, not a run)

The look-back job keeps 12 subtasks through the low phase and goes back to 16 at the rise. A cap on the
trend projection was built to stop that (ISSUES.md #119). Before its first run on the cluster it was
replayed on the decision logs of the eleven on-off runs that exist: the ten above and run 1 of the
stopped policy-fix queue (`python3 cluster/bench/capreplay.py <run>...`, written on 2026-11-22). The
replay recomputes the policy's scale-up threshold, ceil((projection / 0.7 + backlog / 60 s) / capacity
per subtask), and gives the logged threshold in 36 of 36 scale-ups.

- The jobs with the look-back ordered 36 scale-ups of Aggregate from the second high phase on, 39 to
  129 s after the rise. 33 of them came at the job's first evaluation after a gap of more than 45 s
  (median 106 s). The job had scaled down shortly before the rise. Its first reading after the restart
  is compared with the last one before it, so the trend looks like a steep rise, the projection doubles
  the demand (34 of 36) and the threshold is 14 to 23 where the job has 12 (32 of 36).
- The scale-up rests on that one reading. The logs hold 22 later evaluations at the high level, after
  17 of the 36: there the projection is within 3% of the demand and the threshold is 7 to 11.
- Cap, first form (built on 21 Nov): the projection stops at the highest demand the window saw before
  the rise, plus 10%. The recorded job would have held in 26 of 36. In 9 the threshold is 13 where the
  job had 12: the 10% asks for more than the look-back kept.
- Cap, second form (22 Nov): the projection stops at that earlier level itself, or at the demand when
  that is higher. The 10% only decides whether the demand still counts as the earlier level. The
  recorded job would have held in 35 of 36. The one left is a demand 22% above the earlier level, a new
  high, where a scale-up is right.
- Over all 182 evaluations of the 60 high phases from the second on, no threshold of the second form is
  above the size the job had at the phase's first evaluation, except at a new high (6 phases, in 5 of
  which the job is at the largest size already).

Limits. The recorded jobs scaled up at these evaluations, so the readings that follow are those of a job
with 16 subtasks and not of one that held. The second form is in `cluster/plugin` and is not yet run. Its
test on the cluster (five on-off runs, plan amendment of 2026-11-22 01:26 PST) waits for the image.

### The look-back with the trend projection off: the job holds at the returns of the load (pre-test, two runs)

A test of the cause named above that needs no new operator image (plan entry of 2026-11-22 08:47 PST in
`paper/outline.md`, written before the runs; exploratory, two runs, no test). On the image the cluster's
test operator runs, a fifth job has the look-back of ten minutes and its trend projection switched off
by a setting (`trend-horizon-seconds` 0; job `rule+d`). The replay beforehand had said that without the
projection the recorded jobs would have held in 20 of 21 scale-ups. Scenario `onoff` at scale 2, two
runs on 2026-11-22 (16:52Z to 17:46Z), both valid, five jobs side by side. Seconds over the limit and
slots are from the monitoring service (`results-cluster/pretrend/datadog.json`); restarts and orders from
the harness files.

| Job | Over the limit, run 1 / 2 (s) | Of that after the first cycle (harness) | Restarts after the first cycle | Scale-ups of Aggregate ordered from the second high phase on | Slots held |
|---|---|---|---|---|---|
| Built-in rule | 146 / 142 | 10 / 13 | 4 / 4 | not counted | 12.6 / 12.3 |
| Policy | 165 / 174 | 28 / 27 | 8 / 7 | 3 / 2 | 13.0 / 13.1 |
| Policy with look-back (`rule+b`) | 171 / 165 | 27 / 19 | 7 / 7 | 3 / 3 | 13.4 / 13.4 |
| Look-back, cap in its first form, 2 slots (`rule+c`) | 153 / 153 | 1 / 2 | 1 / 2 | 0 / 1 | 11.4 / 11.6 |
| Look-back, projection off (`rule+d`) | 170 / 153 | 16 / 1 | 2 / 1 | 1 / 0 | 11.1 / 11.2 |

- Predictions. P1 held: `rule+d` ordered one scale-up of Aggregate from the second high phase on in the
  two runs together (the look-back alone: three in each). P2 held: it restarted 2 and 1 times after the
  first cycle (the look-back alone: 7 and 7). P3 held: it was not over the limit for longer than the
  look-back job (170 against 171 s, 153 against 165 s). P4 failed as written: its first order was not
  smaller than the others' (4 to 8 subtasks in both runs; the look-back job's 4 to 8 as well, the
  policy's 4 to 8 and 4 to 10). The cost showed elsewhere: its first order came 28 to 29 s into the first
  high phase and not 13 to 14 s, its second asked for 12 and 15 subtasks and not 16, and it was over the
  limit for 5 to 7 s longer in the first cycle than the look-back job (harness: 159 and 156 s against
  152 and 151 s).
- Its one later scale-up (run 1, 417 s, 12 to 16) was not a projection: demand and projected demand
  were both 3,192 records/s. The capacity read 368 records/s per subtask at the job's first evaluation
  after a restart, where the readings before and after are 427 to 462, so the threshold was 13 against
  12 subtasks. At the next evaluation, 16 s later, the threshold was 11. After the scale-down to 12 the
  job held through the four remaining high phases of that run, and in run 2 through all five.
- It held 2.2 slots fewer than the look-back job and 1.9 fewer than the policy, because it stayed at 12
  subtasks where they went back to 16 at every return of the load.
- The job with the cap in its first form held as well in these two runs (0 and 1 scale-ups, 1 and 2
  restarts), where in the one earlier run it had scaled up twice. The replay had given that form 26 of
  36.

What this shows and what not. In closed loop, the scale-up at each return of the load comes from the
trend projection: without it the look-back job held in 9 of 10 later high phases, and the tenth was a
capacity reading. So the look-back with the projection off is a way to stop the flapping with settings
alone on the plugin as it is. It gives up the projection for a first rise too, which cost a later first
order here. The cap in its second form keeps the projection for new highs; it is built (operator image
`cluster10`) and its test (Test B) is unchanged and still to run. Two runs, one load shape, no test.
Reproduce: `python3 cluster/bench/flapsum.py results-cluster/pretrend/onoff/rep*`,
`python3 cluster/bench/flaporders.py results-cluster/pretrend/onoff/rep*`.

### Rate levels

Seconds over the limit and slots in the five runs:

| Job | Over the limit, Datadog | Over the limit, harness | Slots, Datadog |
|---|---|---|---|
| Built-in | 21 (24, 22, 15, 22, 21) | 21 (24, 22, 16, 22, 21) | 14.4 |
| Policy in code | 55 (121, 60, 30, 39, 27) | 58 (123, 63, 34, 41, 30) | 14.1 |
| Policy with look-back | 17 (9, 6, 6, 48, 16) | 19 (9, 6, 6, 56, 16) | 15.8 |
| LLM | 164 (146, 168, 150, 186, 172) | 171 (156, 171, 151, 193, 183) | 13.9 |
| LLM with history | 4 (3, 7, 1, 6, 2) | 4 (3, 6, 1, 5, 4) | 15.9 |

- The two jobs that look back stayed at 15 to 16 subtasks for nearly the whole run, because every 10
  minutes of this load hold a high level. The LLM with history changed Aggregate at most once per run, the
  policy with look-back three times on average. The LLM with history was late for 1 to 7 s, the policy
  with look-back for 6 to 16 s in four runs and for 48 s in the run in which the sink's close error stopped
  two of its TaskManagers. At that point they are close to a job that is not scaled at all.
- What decides a run for the other two jobs is the 600 level and the jump to 3,600 that follows it. The LLM
  went down in two steps in all five runs (16 to 10 or 12 about 35 s after the drop, then to 5 or 6 about
  200 s after it) and stood at 5 or 6 when the load jumped: 120 to 166 s over the limit in that level
  alone. The policy did the same in run 1 (105 s). In runs 2 to 5 it took a small step first, 17 to 24 s
  after the drop (the sink from 4 to 2 in three runs, Aggregate from 16 to 15 in two), then waited out its cooldown
  of 150 s, went to 8 in one step about 170 s after the drop, and stood at 8 at the jump: 7 to 21 s.
- Both jobs took the policy's choices. The LLM chose what the policy chose in 557 of 560 choices. The
  difference is when the choices were made. The measured demand falls from 4,800 to 600 over the
  one-minute window. A decision 17 to 24 s after the drop saw 2,860 to 3,270 records/s, where the sizing
  still gives 13 to 15 subtasks for Aggregate, and made the small change. Its cooldown then kept the job
  large until it was too late for two steps down, so the small first step protects the job by accident.
  A job whose decisions fell at 13 to 19 s and at 28 to 38 s saw 3,680 to 4,040 records/s (no change) and
  then 2,170 to 2,400 (10 to 12 subtasks), and went down twice. The policy's job fell on the first
  pattern in four of five runs, the LLM's job on the second in five of five. The policy's job decides
  every 15 s and the LLM's every 17.7 s (median; a call takes 2.6 to 2.8 s). The LLM job's decisions
  fell at the same offsets from the drop in all five runs (0, 18 and 36 s, within 2 s). We have not found
  why; the harness delays the load changes by a random time per run, and the on-off runs show no such
  alignment. So the five runs are not five independent draws of the timing, and the result says that the
  outcome on this load turns on seconds of timing, not that a model is worse.
- The built-in rule's delay let it take one step down (to 8 or 10) before the load rose: 6 to 21 s late in
  that level, 15 to 24 s in the whole run, with a quarter of a slot more than the policy.
- The LLM with history chose more than the policy in 384 of 828 choices.
- The sink's close error hit four jobs in the five runs: the policy in run 2 (two TaskManagers at a
  scale-down from 16 to 15: 8 subtasks for 35 s and 36 of its 60 late seconds), the LLM in run 1 (one
  TaskManager at a scale-up from 6 to 15, reached 25 s late), and in run 4 the policy with look-back (two
  TaskManagers at a restart 56 s after the jump: 8 subtasks for 25 s, 51 of its 56 late seconds) and the
  policy (two TaskManagers at a scale-down from 16: 8 subtasks for 35 s, about 20 of its 41 late seconds).
- In run 5 the operator's log as fetched at the end began 730 s into the run. Its decision log was rebuilt
  from the copy the harness now follows during a run (`cluster/bench/rebuild_decisions.py`, ISSUES.md #109).

What this shows and what it does not:

- The complaint holds for our own policy. On a load that returns within two minutes it restarts every two
  to three minutes, lands most of its scale-downs when the load is back, and frees no pods.
- Looking back 10 minutes helps where the cost is a lack of capacity (rate levels: 4 and 17 s late on
  average against 55 and 164 s) and is paid in slots (16 against 14). A rule in code does it as well as the instruction in
  the prompt. The LLM did not find the remedy in the history: we wrote the remedy into the prompt. A
  prompt with the history and no instruction was not tested.
- Neither form of looking back removed the restarts on the on-off load. The code version is undone by the
  policy's own trend projection, the prompt version by the restarts for the sink. Both are fixable in the
  policy and are not fixed here.
- The built-in rule's plain scale-down delay did best on both loads for what it costs. The plugin's policy
  has a cooldown after a change and no delay before a first scale-down.
- With the policy's choices in both jobs, the LLM's job was late about three times as long as the policy's
  on the rate levels (164 against 55 s). The cause is the timing of the decisions after the drop, not a
  choice of the model: on a load like this the outcome turns on which reading of a falling rate a
  decision sees. A first scale-down that is delayed or sized for a full window would remove that
  dependence (ISSUES.md #107).
- Exploratory: five runs of each load, one phase length, one window length, a high level that half the
  capacity can still carry. Differences of 10 s between jobs on the on-off load are within the spread.

A try at two remedies in the policy was stopped after one run (2026-11-21, `results-cluster/policyfix/`,
ISSUES.md #119): the cap on the trend projection did not act, because the reading at the jump was 3.5%
above the window's earlier peak. No claim rests on that run.

Data: `results-cluster/flapping/` (harness files and saved JobManager and TaskManager logs, not published),
`results-cluster/flapping/datadog.json` (whole runs, and the on-off runs from the second high phase).
The five on-off runs with the newer connector: `results-cluster/fix105/` (harness files, `datadog.json`,
`fixcheck.txt`).
Harness views: `python3 cluster/bench/flapsum.py results-cluster/flapping/onoff/rep*`,
`python3 cluster/bench/flapphase.py <run>`, `python3 cluster/bench/expsum.py results-cluster/flapping`.
Queue: `cluster/bench/queue_flapping.sh`.

---

## 11. The band of the built-in rule: acting sooner did not shorten the time over the limit (E3, 20 runs; E3b, 6 runs)

The built-in rule acts when a vertex's utilization leaves a band around the target of 70%. With the
shipped settings the upper bound is 100%, so it acts at saturation. In the main campaigns the built-in rule
runs with the upper bound at the target, as the policy in code has it. E3 puts the bands side by side:
shipped, narrow (50 to 80%), at the target, and the policy in code; spike, ramp, scale-down and slow-down,
5 runs each, default signal. E3b runs the shipped band, the band at the target, the policy and the LLM in
the eight-minute overload, 6 runs. No test was planned for either; all figures are intervals.

Seconds over the limit / slots, means:

| Scenario | Shipped band | Narrow band | Band at the target | Policy in code | LLM |
|---|---|---|---|---|---|
| Ramp (5 runs) | 49 / 3.3 | 23 / 4.2 | 30 / 4.0 | 32 / 5.3 | |
| Spike (5) | 168 / 4.1 | 171 / 5.0 | 178 / 4.8 | 144 / 4.9 | |
| Scale-down (5) | 2 / 7.4 | 2 / 7.2 | 2 / 7.4 | 4 / 5.8 | |
| Slow-down (5) | 0 / 4.0 | 7 / 6.0 | 9 / 5.1 | 17 / 4.8 | |
| Overload (E3b, 6) | 470 / 5.4 | | 462 / 5.4 | 322 / 5.8 | 356 / 5.8 |

- The shipped band acts last: 142 s into the ramp against 92 s with the band at the target, 51 s into
  the spike against 26 s, 37 s into the overload against 23 s. It was not over the limit for longer.
  Pooled over the four scenarios of E3 it differs from the band at the target by +3% of a scenario's mean
  [-18, +23], with 15% fewer slots [-21, -9]. In the overload the difference is -8 s [-88, +73] in favour
  of the band at the target, with the same slots. In the slow-down the shipped band never rescaled and
  was never over the limit (0 s against 9 s).
- The narrow band: -8% [-23, +7] seconds over the limit and 5% more slots [+1, +10] than the band at
  the target.
- The policy in code against the band at the target, pooled over E3: +5% [-12, +22] seconds over the
  limit and the same slots (0% [-10, +10]). By scenario it differs: 35 s less in the spike [-43, -26], in
  5 of 5 runs; 1.6 slots fewer after the scale-down [-2.6, -0.7]; 1.3 slots more in the ramp for the
  same time over the limit; 8 s more in the slow-down [-0.2, +15.8], where it scales up and down again.
- In the overload the policy was over the limit for 149 s less than the shipped band [-187, -110] and
  the LLM for 114 s less [-170, -58], each in 6 of 6 runs, with 0.4 slots more. Section 5 shows where
  that comes from: the policy's gain over the built-in rule is a gain over the stale signal and is not
  detectable with the broker-side signal. LLM against policy: +34 s [-33, +102]; the LLM chose what the
  policy chose in 230 of 233 choices.
- So an earlier reaction is a matter of configuration, and in these two campaigns it did not help by
  itself: where both bands scale up, the band at the target reacts 14 to 50 s sooner than the shipped band,
  and it is not detectably better on any of the five loads. What the band changes here is the slots. The
  one case in which acting earlier helped is not part of them: the 40-minute overload with five-minute
  windows and the default signal, -200 s [-275, -126] (section 2, a comparison that was not planned).

Data: `results-e3/`, `results-e3b/`. Numbers: `paper/gen/summary_e3.txt`, `paper/gen/summary_e3b.txt`.
Reproduce: `bench/campaign.sh e3`, `bench/campaign_e3b.sh`, then `python3 bench/paper.py e3` and
`python3 bench/paper.py e3b`.

---

## 12. On the cluster the backlog metric shows about a tenth of the backlog, as on the laptop (68 job-runs of the cluster test, not planned)

The cluster test was built to compare controllers. Its sample files also hold what section 1 needs: every
five seconds per job the partitions' end offsets (read from the brokers), the readers' positions and
`pendingRecords` (read from the job). A reviewer of the draft pointed this out on 2026-11-21; the check
was not planned. Setup of these runs: a managed Kafka service with three brokers, replication factor 3,
a real network, 16 partitions on 4 source subtasks (4 partitions per reader), default fetch size, records
of about 40 bytes, and two builds of the job (Kafka client 3.9.1 with connector 4.0.1, client 4.2.0 with
connector 5.0.0).

Share of the true backlog (end offsets minus positions) that the metric shows, over all samples of a
job-run with a true backlog of at least 20,000 records (`--min 20000`). With the script's default of 5,000
records six more job-runs appear, informed jobs with one or two samples at a restart, where the metric
reads zero; they are not job-runs with a sustained backlog:

| Runs | Client | Job-runs | Share shown |
|---|---|---|---|
| Announced spike, the three jobs that were told nothing, 6 runs (800 to 4,000 records/s) | 3.9.1 | 18 | 7.8 to 10.1% |
| On-off load, all five jobs, 5 runs (the first high phase holds nearly all of the backlog) | 3.9.1 | 25 | 6.3 to 10.8% |
| The same, 5 runs | 4.2.0 | 25 | 6.8 to 10.2% |

- The largest backlog of a job-run is between 56,000 and 211,000 records. Before the load rises, end
  offsets minus positions has a median of -90 to -29 records per job-run at 800 records/s, so the two
  readings are about 0.1 s apart and the backlog is not an artefact of the sampling.
- The laptop's open-loop cell with 4 partitions per reader and the default fetch showed 7% (section 1).
  The model of section 1, for one broker and the rates of these runs (3,200 or 4,000 records/s arriving,
  about 1,780 read), gives 6 to 9%, with either fetch rule.
- What this is: the first measurement off the laptop, with several brokers, a real network and a 4.x
  client, and it agrees with the laptop in size. What it is not: an open-loop grid, a test of the model
  with three brokers, or a statement about partitions that deliver megabytes per second. The jobs were
  under an autoscaler, so each backlog lasted one to three minutes.

Reproduce: `python3 cluster/bench/signalcheck.py --min 20000 results-cluster/announced/spike/rep* results-cluster/flapping/onoff/rep* results-cluster/fix105/flapping/onoff/rep*`
(68 job-runs, 6.3 to 10.8%, median 9.0%).

## 13. On the cluster, in closed loop, the broker-side signal halved the time over the limit, and cut it by a quarter with five-minute windows (cluster signal campaign, 8 and 6 runs, planned)

Sections 2 and 3 are from one laptop with one broker. This is the closed-loop counterpart of E8b on the
shared cluster. Design, analysis and five predictions were written and hashed before the first run
(`paper/outline.md`, entry of 2026-11-21 13:10 PST; `paper/plan-snapshots/outline-2026-11-21T1310.md`).

Setup: scenario `surge` at scale 2 (400 records/s, then 4,800 records/s for 8 minutes, then 400), four
built-in jobs side by side: shipped band and band with its upper bound at the target, each with the
default signal and with backlog and arrival rate read from the brokers. Source fixed at 4 subtasks, 16
partitions (4 per reader), default fetch size, one-minute window and catch-up time, Flink 2.3.0, connector
5.0.0, client 4.2.0, three brokers. Eight runs on 2026-11-21, 21:13Z to 23:27Z, one after the other. The
seconds over the latency limit and the slots are from the monitoring service
(`results-cluster/signal/datadog.json`); the counts are from the harness's files.

Seconds over the limit of 2 s per run (monitoring service), and the sizes Aggregate went through:

| Run | Shipped band, default | Shipped band, broker-side | Band at the target, default | Band at the target, broker-side |
|---|---|---|---|---|
| 1 | 512 (2, 5, 12, 16) | 185 (2, 16) | 511 (2, 3, 8, 16) | 220 (2, 16) |
| 2 | 514 (2, 5, 10, 16) | 159 (2, 16) | 492 (2, 4, 10, 16) | 162 (2, 16) |
| 3 | 508 (2, 5, 12, 16) | 177 (2, 16) | 517 (2, 3, 6, 16) | 194 (2, 16) |
| 4 | 519 (2, 5, 10, 16) | 188 (2, 16) | 505 (2, 4, 8, 16) | 188 (2, 16) |
| 5 | 438 (2, 6, 16) | 353 (2, 4, 16) | 524 (2, 3, 8, 16) | 376 (2, 3, 16) |
| 6 | 482 (2, 5, 16) | 355 (2, 4, 16) | 509 (2, 4, 8, 16) | 365 (2, 4, 16) |
| 7 | 525 (2, 5, 10, 16) | 177 (2, 16) | 498 (2, 3, 8, 16) | 175 (2, 16) |
| 8 | 480 (2, 5, 14, 16) | 191 (2, 16) | 461 (2, 4, 12, 16) | 195 (2, 16) |
| mean | 497 | 223 | 502 | 234 |

Primary analysis (broker-side minus default within each band, paired within runs, exact sign-flip test,
Holm over the two tests):

- Shipped band: -274 s [95% interval -363, -185], 55% of the default-signal job's time, lower in 8 of 8
  runs, p = 0.016 after correction (the smallest value that 8 pairs and two tests allow).
- Band at the target: -268 s [-333, -203], 53%, lower in 8 of 8 runs, p = 0.016.
- The harness's own latency files give -278 s and -276 s.

Reported without tests:

- Slots held: 12.8 against 10.2 with the shipped band (+2.6 [1.5, 3.7]) and 12.8 against 9.9 with the band
  at the target (+2.9 [2.3, 3.5]). The broker-side jobs run at 16 subtasks for most of the overload.
  These slots are a floor for the price of the fix: 16 is the maximum, and the rule's formula asked for
  more. The plugin logs the formula's size at every evaluation, before the maximum is applied
  (`rule_size` in `decisions.jsonl`). When that size first passed 16 while Aggregate still ran below it,
  it was 20 to 23 subtasks in the twelve job-runs with one step (28 to 34 s after the rise) and 48 to 50
  in the four job-runs with the early small step (114 to 116 s after it); the median of the 16 is 21. It
  peaked at 31 to 50. With five-minute windows (next subsection) it was 41 to 43 in all twelve job-runs.
  Not planned; `python3 cluster/bench/sigsum.py results-cluster/signal surge` prints it (added 22 Nov, after
  review round 3 asked for it).
- Scale-ups of Aggregate: 1.25 against 2.75 and 1.25 against 3.00. The default-signal job needed three
  steps in 6 of 8 runs with the shipped band and in 8 of 8 with the band at the target. The broker-side
  job needed one step in 6 of 8 runs in each band and two in the other 2.
- Aggregate ran with the 10 subtasks that cover the load 86 s and 87 s after the load rose with the
  broker-side signal, 210 s and 258 s with the default signal. With the band at the target the
  default-signal rule acts sooner (17 to 36 s after the load rises, against 41 to 51 s) and is ready
  later, as in section 11.
- All 16 broker-side jobs caught up before the overload ended (latency under the limit for 20 s from 173
  to 234 s after the load rose in six runs, from 365 to 405 s in runs 5 and 6). Of the 16 default-signal
  jobs one did, 21 s before the load fell.
- Restarts counting every vertex: 1.75 against 3.00 and 2.12 against 3.75.

Runs 5 and 6. Both broker-side jobs took a small first step there, to 3 or 4, and a second one to 16. No
read from the brokers failed. The first evaluation fell 4 to 7 s after the load rose. The backlog was
then 16,000 to 25,000 records, the plugin reported it, and the rule acted on it with a demand average
that still held mostly the old load (target rates of 601 to 802 records/s). A step to 3 or 4 fits in the
slots the job already holds, so it took effect at once, the metrics were cleared, and the rule could not
decide again until 114 to 116 s after the load rose. In the other six runs the rule's first order came
12 to 17 s after the load rose and asked for 8 or 10 (in run 4 an evaluation 1 s after the rise saw too
little to act). That needs new TaskManager pods. While they started, the next two evaluations raised the
order to 14 or 15 and then 16, and the job restarted once, at 16, 45 to 65 s after the first order. So a
true signal can make the rule act too early, and a rescale that waits for pods gets corrected for free
while one that runs at once locks the rule out for a stabilization interval and a window. The same cost
showed on the laptop in the overload to 1,200 records/s with the band at the target (section 2).

The five predictions:

1. Shipped band, lower in at least 7 of 8 runs and by at least 20% on average: held (8 of 8, 55%).
2. Band at the target, at least 7 of 8 and 25%: held (8 of 8, 53%).
3. `pendingRecords` under 20% of the true backlog in the default-signal jobs during the overload: failed
   as written. Over all samples with a true backlog of at least 20,000 records the 16 default-signal
   job-runs show 18 to 41%, one of them under 20%. Split by the size of the job at the sample (not
   planned), the metric shows 4 to 5% while Aggregate runs at 2 subtasks and 5 to 12% at 3 to 6, which
   is where the rule decides its first steps, and 24 to 55% once it runs at 7 or more and the readers
   are throttled less. The prediction did not say that the share moves with the throttling, which
   section 1 states.
4. The broker-side jobs hold more slots in both bands: held.
5. The broker-side jobs need no more scale-ups of Aggregate in either band: held (never more in any
   run; equal in runs 5 and 6 with the shipped band).

Checks: no run was excluded or repeated. The operator logged no failed read of the end offsets in any
run. 87 restarts in the 8 runs, no TaskManager left without a release request, no fatal exit
(`fixcheck.py`, run in the driver pod, where the JobManager and TaskManager logs are; they are not copied
out, and the harness's samples count 85 restarts); 57 changes of parallelism in the samples, none with a lost TaskManager (`tmdrop.py`).
The latency series covers 97.6 to 99.4% of the seconds of each job. The monitoring service and the
harness differ by at most 15 s per job-run in the seconds over the limit.

What this is not. One scenario on one afternoon, with a one-minute window and catch-up time, a job whose
work is simulated, and a source fixed at 4 subtasks. Four jobs run side by side on a shared cluster. The
gain is larger than on the laptop (35% and 42%), and we have not tested why. One candidate: on the
cluster the default-signal rule needed three steps where the broker-side rule needed one, and on the
laptop the counts were closer (section 2). The follow-up with five-minute
windows (plan entry of 14:06 PST) is below.

Reproduce: `python3 cluster/bench/sigsum.py results-cluster/signal surge`,
`python3 cluster/bench/scalesteps.py results-cluster/signal/surge/rep*`,
`python3 cluster/bench/signalcheck.py --min 20000 --by-size results-cluster/signal/surge/rep*`,
`python3 cluster/bench/tmdrop.py results-cluster/signal`.

### The same with five-minute windows (6 runs, planned)

Design, one primary test and four predictions were written and hashed before the first run
(`paper/outline.md`, entry of 2026-11-21 14:06 PST; `paper/plan-snapshots/outline-2026-11-21T1406.md`).
This is E8c of the laptop on the cluster.

Setup: scenario `surge_long` at scale 2 (400 records/s, then 4,800 records/s for 40 minutes, then 400),
the same four jobs, images and cluster. Five-minute metric window, two-minute stabilization, five-minute
catch-up time, ten-minute scale-down interval. Six runs of 48 measured minutes, 2026-11-21 23:36Z to
2026-11-22 05:18Z, one after the other.

Seconds over the limit of 2 s per run (monitoring service), and the sizes Aggregate went through:

| Run | Shipped band, default | Shipped band, broker-side | Band at the target, default | Band at the target, broker-side |
|---|---|---|---|---|
| 1 | 1,608 (2, 4, 16) | 1,229 (2, 4, 16) | 1,821 (2, 3, 12, 16) | 1,253 (2, 3, 16) |
| 2 | 1,625 (2, 4, 16) | 1,198 (2, 4, 16) | 1,860 (2, 3, 12, 16) | 1,283 (2, 3, 16) |
| 3 | 1,550 (2, 4, 16) | 1,211 (2, 4, 16) | 1,695 (2, 3, 14, 16) | 1,249 (2, 4, 16) |
| 4 | 1,944 (2, 3, 12, 16) | 1,212 (2, 4, 16) | 1,606 (2, 3, 15, 16) | 1,232 (2, 3, 16) |
| 5 | 1,644 (2, 4, 16) | 1,217 (2, 4, 16) | 1,609 (2, 3, 15, 16) | 1,296 (2, 3, 16) |
| 6 | 1,594 (2, 4, 16) | 1,320 (2, 3, 16) | 1,538 (2, 3, 15, 16) | 1,308 (2, 3, 16) |
| mean | 1,661 | 1,231 | 1,688 | 1,270 |

Primary analysis (one test, no correction): shipped band, broker-side minus default, paired within runs:
-430 s [95% interval -597, -263], 26% of the default-signal job's time, lower in 6 of 6 runs, exact
sign-flip p = 0.031 (the smallest value that 6 pairs allow). The harness's own latency files give -429 s.

Reported without tests:

- Band at the target: -418 s [-564, -272], 25%, lower in 6 of 6 runs (harness: -419 s).
- Slots held: 13.3 against 12.6 with the shipped band (+0.7 [0.5, 0.9]) and 13.3 against 12.6 with the
  band at the target (+0.6 [0.4, 0.9]).
- Scale-ups of Aggregate: 2.00 against 2.17 and 2.00 against 3.00. With the band at the target the
  default-signal job needed three steps in 6 of 6 runs.
- Aggregate ran with the 10 subtasks that cover the load 516 s and 502 s after the load rose with the
  broker-side signal, 652 s and 582 s with the default signal (-136 s [-156, -115] and -80 s [-98, -62]).
- Restarts counting every vertex: 3.00 against 2.33 and 3.00 against 4.00. The third restart of the
  broker-side jobs is a scale-down of the second operator from 4 to 3 subtasks about 26 minutes after
  the rise, inside the overload.

The four predictions:

1. Shipped band, fewer seconds in 6 of 6 runs and by at least 10% on average: held (6 of 6, 26%).
2. Band at the target, fewer in at least 5 of 6: held (6 of 6).
3. The same number of scale-ups of Aggregate in most pairs, and with the shipped band the default
   signal's first one later: held in part. With the shipped band the numbers are equal in 5 of 6 pairs
   and the default signal's first order came 154 to 195 s after the rise against 24 to 36 s. With the
   band at the target they differ in 6 of 6 pairs (3 against 2), so over the 12 pairs "most" fails.
4. The default-signal jobs' `pendingRecords` shows a larger share of the true backlog than in the
   shorter runs: held. Over all samples with a true backlog of at least 20,000 records the 12
   default-signal job-runs show 54 to 64% (18 to 41% in the eight-minute runs). The backlog grows to
   about 2 million records and the job runs at 16 subtasks for most of the overload, where the metric
   shows 66 to 74%. Split by the job's size (not planned) it shows 5% at 2 subtasks and 15 to 34% at
   3 to 6, which is again where the rule decides.
Neither outcome that the plan names as counting against the finding occurred (fewer than 5 of 6 runs,
or a mean change under 5%).

Not planned: the small first step is the rule here. All 24 job-runs went from 2 to 3 or 4 subtasks
first. With the broker-side signal that order came 14 to 36 s after the rise, at a target rate of 592 to
910 records/s, because the five-minute average had hardly moved. The step fits in the slots the job
holds and takes effect at once. The next order, to 16, came 450 to 486 s after the rise: one window plus
the stabilization interval later. With the default signal the first order came after 92 to 195 s and
the next after 529 to 637 s. So with long windows the broker-side signal still wins: the load is covered
80 to 136 s sooner and the backlog to work off is smaller (1.5 to 1.7 million records at its largest
against 2.0 to 2.2 million). But both lose about eight minutes to a first step that was sized on an
average of old load. This is the problem of runs 5 and 6 above (ISSUES #123), in every run.
A replay of the step-aware demand on these runs (`stepreplay.py`; not a run of the fix) gives 4,672 to
4,982 records/s at the evaluation where the job ordered 3 or 4 (4,295 in one job-run), which sizes
Aggregate at 16 (14 in that one). A test of that change with these windows was added to the plan before
any run with it (Test C, entry of 21:31 PST).

Checks: no run was excluded or repeated. The operator logged no failed read from the brokers in any
run. This was read in the logs that the harness followed during the runs; the log fetched at the end of
run 5 starts 25 minutes late after a rotation (ISSUES #126). The harness marked no job invalid and the
latency series covers 99.4 to 99.8% of the seconds of each job. 118 changes of parallelism in the samples
of all 14 runs of the campaign, none with a lost TaskManager (`tmdrop.py`). `fixcheck.py` in the driver
pod finds no TaskManager gone without a release request and no fatal exit; its 61 restarts are
incomplete, because followers of pod logs ended early in runs 2 and 5 (ISSUES #127; the samples count
74). The monitoring service and the harness differ by at most 10 s per job-run in the seconds over the
limit.

What this is not. The same limits as above, with one more: six runs in one night. The laptop's E8c gave
-377 s of 1,475 s (26%) with the shipped band and -189 s of 1,275 s (15%) with the band at the target.

Reproduce: `python3 cluster/bench/sigsum.py results-cluster/signal surge_long`,
`python3 cluster/bench/scalesteps.py results-cluster/signal/surge_long/rep*`,
`python3 cluster/bench/signalcheck.py --min 20000 --by-size results-cluster/signal/surge_long/rep*`,
`python3 cluster/bench/stepreplay.py results-cluster/signal/surge_long/rep*`.

### The same at the operator's default time settings (5 runs, exploratory by the plan's rule, no test)

Plan entry of 22 Nov 09:06 PST with the amendment of 14:30 PST; checks and outcome entries of 23 Nov
12:12 and 12:14 PST. The operator's defaults for the five time settings the other campaigns shorten
(metric window 15 minutes, stabilization 5 minutes, catch-up 30 minutes, restart time 5 minutes,
scale-down interval 1 hour), 400 to 4,800 records/s for 90 minutes after a warm-up of 21 minutes, the
same four built-in jobs side by side. Up to six runs were planned until 23 Nov 12:00 PST; five are
complete, so there is no test. In run 1 the cluster's node autoscaler evicted a TaskManager of every job
227 s before the load fell; the run is kept and marked.

| | default signal | broker-side signal | difference [95%] | lower in | slots held |
|---|---|---|---|---|---|
| shipped band | 4,183 s | 3,179 s | -1,004 s [-1,101, -907], 24% | 5 of 5 | 12.3 / 13.0 |
| band at the target | 3,971 s | 3,317 s | -654 s [-799, -510], 16% | 5 of 5 | 12.6 / 13.1 |

Without run 1: -991 s [-1,125, -857] and -646 s [-857, -435], 4 of 4 each. By run with the shipped band:
-1,055, -901, -972, -988, -1,104 s (22 to 26%). No job was still over the limit at the end.

What happened in every run. With either signal the rule took two steps: 2 to 4 subtasks and then to
the maximum of 16 with the shipped band, 2 to 3 and then 16 with the band at the target. With the
broker-side signal and the shipped band the first order came 93 to 101 s after the rise and the second
1,308 to 1,316 s after it; with the default signal they came after 426 to 488 s and after 1,646 to
1,708 s. The load was covered 374 s [341, 407] sooner. So the signal makes the rule act about six
minutes sooner, twice; the step is the same, and after it the rule cannot decide for 20 minutes. Both
jobs were over the limit for most of the 90 minutes. The second order of the broker-side jobs was cut by
the maximum (the formula asked for 28 to 31 subtasks in the six of ten job-runs whose decision log
covers that evaluation). `pendingRecords` showed 6 to 8% of the true backlog over the first ten minutes
in four runs and 15% in one (default signal, shipped band). Not planned, and the reason why the
default-signal job needed no third step: while it ran with 2 subtasks its metric showed 6 to 7% of the
backlog, and once it ran with 4 it showed 71 to 73% (52 to 81% by sample), in all five runs. Its second
order was therefore sized on most of a backlog of millions of records and went to the maximum.

Predictions: D1 (lower in every run, by 600 s or more, 20% on average), D2 (first order and time of the
full size with the broker-side signal), D5 (more slots) and D6 (band at the target) held. D4 held in 4
of 5 runs (15% where under 15% was predicted). D3 failed in all five: its first part held (first order
after more than 300 s), its second did not; we had worked out three steps for the default-signal job
and it took two. The clause against the finding is not met.

Not planned: with the broker-side signal the band at the target was over the limit for 138 s
[117, 159] longer than the shipped band, in 5 of 5 runs (it acts 45 s sooner and orders 3 subtasks where
the shipped band orders 4); with the default signal it was 212 s [10, 414] shorter, in 4 of 5.

What this is not. Five runs on one cluster with one load shape and no test: it supports the sign and
the rough size at the default settings. The demand that follows a jump (section 16) was not run at these
settings; it is the remedy for the small first step that costs both jobs most of their time here.

Reproduce: `python3 cluster/bench/defaultscore.py results-cluster/defaultwin` (add `--without 1`),
`python3 cluster/bench/sigsum.py results-cluster/defaultwin surge_default --no-test`,
`python3 cluster/bench/disturbed_cluster.py results-cluster/defaultwin surge_default`,
`python3 cluster/bench/readfail.py results-cluster/defaultwin/surge_default/rep*` (from `cluster/bench`).

## 14. Five short scenarios: no planned test separates the policy from the built-in rule or the models from the policy (main campaign, 80 runs)

Five scenarios with a measured phase of 6 or 7 minutes (ramp, spike, drop, skew, slow-down), four
controllers side by side in every run, 8 runs per scenario with the default fetch size and 8 with a 4 KiB
fetch. Tests, margins and wording were fixed before the data were read (`paper/outline.md`, amendment of
2026-11-21 13:27 PST; checks entry of 2026-11-22 01:16 PST; outcome entry of 07:10 PST).
Numbers: `paper/gen/summary_main.txt`, `results-v3/runs.csv`; `python3 bench/paper.py main`, then
`python3 bench/main_marked.py`.

Planned tests: paired differences within runs, divided by the scenario's mean (at least 30 s or half a
slot), pooled over the five scenarios of an arm, 95% Student's t interval, exact sign-flip test, Holm over
the six tests of an arm.

| Arm | Contrast | Time over the limit | adj. p | Slots | adj. p |
|---|---|---|---|---|---|
| Default fetch | Policy - built-in | -6.1% [-17.9, +5.7] | 0.81 | -1.7% [-8.3, +4.9] | 0.81 |
| | Decision model - policy | -15.7% [-31.4, -0.1] | 0.12 | +1.5% [-0.5, +3.4] | 0.54 |
| | LLM - policy | -16.4% [-33.5, +0.7] | 0.24 | +1.4% [-1.1, +3.8] | 0.81 |
| 4 KiB fetch | Policy - built-in | -4.8% [-19.9, +10.4] | 1.00 | -3.3% [-9.7, +3.1] | 1.00 |
| | Decision model - policy | -6.4% [-16.6, +3.9] | 1.00 | +1.1% [-2.2, +4.5] | 1.00 |
| | LLM - policy | -0.5% [-9.7, +8.6] | 1.00 | +3.4% [+0.5, +6.3] | 0.12 |

Correction of 2026-11-22 (review round 3, ISSUES.md #132): the first write-up of this table, a few hours
earlier, printed bootstrap intervals (bias-corrected and accelerated), because the generator switched to
them above 30 values and these pooled contrasts have 40 pairs. The plan says Student's t intervals, and
under the test's own null the bootstrap interval of this statistic excluded zero in 14 to 20% of draws
where the t interval did in 2 to 4%. The table now has the t intervals (`boot_ci` in `bench/paper.py`
returns them for any number of values), and the sign-flip test is exact up to 44 pairs (0.25 became
0.24). The bootstrap intervals were [-19.0, +4.1], [-39.3, -5.1], [-38.4, -3.8] for the time with the
default fetch and [-20.5, +8.4], [-19.8, +1.2], [-8.4, +9.2] with the 4 KiB fetch; `main_marked.py`
still prints them next to the t intervals, marked as not used.

- No test is significant after Holm in either arm. Three unadjusted p values are below 0.05: both models
  against the policy on the time with the default fetch (0.020, 0.048), and the LLM on the slots with the
  4 KiB fetch (0.020).
- By the wording rule both models get "no detectable difference": the intervals allow a model's job to be
  over the limit for up to 34% less than the policy's (31% for the decision model, 34% for the LLM) or 9%
  more. Neither is called not distinguishable: 1 of the 4 intervals of the decision model and 2 of the 4
  of the LLM lie inside the margins (10% of the time, 4% of the slots). "The models add nothing" is not
  supported by this campaign.
- The intervals themselves are not corrected. With the default fetch the decision model's interval for
  the time ends at -0.1% and the LLM's includes zero. 24 of the 40 pairs behind them are divided by the
  floor of 30 s (16 of 40 with the 4 KiB fetch; none for the slots). By scenario the
  decision model differs from the policy by -0.1 s (ramp), +3.1 s (spike), -0.25 s (drop), -16.9 s
  [-41.0, 7.3] (skew) and -7.0 s [-12.7, -1.3] (slow-down, lower in 7 of 8 runs). In the skew two of the
  eight runs carry the difference (policy 85 and 88 s, decision model 28 and 19 s, LLM 41 and 13 s). With
  the 4 KiB fetch the same two scenarios give -4.9 s and +0.1 s.
- By day and by night (a check that was not planned): with the default fetch the decision model's
  difference is -23.6% [-51.2, +4.1] in the 15 day runs and -11.2% [-31.6, +9.1] in the 25 night runs;
  the LLM's is -29.9% [-53.0, -6.7] and -7.9% [-32.1, +16.3].
- Without the eight runs that carry a testbed event (seven task failures and one lost TaskManager, all
  made by day; seven of the eight events lie outside the load change, see the correction below): no test
  becomes significant. One margin check changes:
  the LLM's interval for the time with the 4 KiB fetch is no longer inside 10%.
  Default fetch: -6.2% [-18.6, +6.2], -16.4% [-32.9, +0.1], -17.0% [-35.0, +1.0] for the three contrasts
  on the time; 4 KiB fetch: -12.5% [-35.1, +10.1], -4.3% [-14.3, +5.7], -3.2% [-11.9, +5.5].

The policy against the built-in rule, by scenario (seconds and slots, uncorrected):

| Scenario | Default fetch | 4 KiB fetch |
|---|---|---|
| Ramp | -28.1 s [-44.5, -11.7], 8 of 8; +1.3 slots [1.0, 1.6] | -19.0 s [-40.7, 2.7], 6 of 8; +1.1 slots [0.68, 1.6] |
| Spike | -27.0 s [-41.1, -12.9], 7 of 8; -0.02 slots | -15.8 s [-54.7, 23.2], 5 of 8; -0.06 slots |
| Drop | +2.4 s [1.0, 3.7]; -1.8 slots [-2.4, -1.2] | +5.8 s [-3.5, 15.0]; -1.6 slots [-2.2, -0.97] |
| Skew | +1.4 s [-7.4, 10.1]; +0.14 slots | -16.5 s [-43.7, 10.7]; -0.11 slots |
| Slow-down | +8.5 s [4.2, 12.8], higher in 8 of 8; -0.66 slots [-1.1, -0.24] | +8.5 s [0.08, 16.9], lower in 0 of 8; -0.75 slots [-1.2, -0.25] |

- The pooled figure is near zero because the scenarios pull in opposite directions. The policy is ahead
  where the load rises (ramp, spike), for 1.3 slots more on the ramp. It gives capacity back sooner after
  the drop and in the slow-down, holds 0.7 to 1.8 slots fewer there, and is over the limit a little
  longer.
- The interval of the spike with the 4 KiB fetch includes zero, so by the scope rule the sentence that the
  policy's gain is a gain over the default signal is not limited to the sustained overload. The 4 KiB
  estimates on ramp and spike are smaller than those with the default fetch and not small, and the arms
  are separate runs.

Decisions.
- In the measured phases, by the plan's definition of 18 Nov, the decision model's most likely option was
  the policy's in 1,951 of 1,968 decisions (17 below, 6 of them applied) and the LLM's in 1,766 of 1,770
  (3 above, 1 below, all applied); `cluster/bench/decount.py` gives the same. With the warm-up included:
  1,634 of 1,645 and 1,594 of 1,604 for the decision model in the two arms (21 below), 1,438 of 1,438
  and 1,397 of 1,401 for the LLM.
- For Aggregate in the measured phases the decision model differed 5 times, all with the default fetch.
  Four times it held 4 or 6 subtasks where the policy scaled up to 6 or 8 (busy 62 to 80%, no backlog;
  two skew runs, two slow-down runs). In those four runs its job was over the limit for 28, 4, 9 and
  14 s and the policy's for 85, 4, 19 and 23 s.
- The situation the LLM's prompt names (Aggregate busy at least 95%, believed demand below capacity, a
  larger size on offer) arose in 4 cycles with the default fetch; the policy scaled up in all 4 and the
  LLM chose the policy's size. With the 4 KiB fetch it arose in 5 cycles; the policy scaled up in 4 and
  the LLM went above it in 2, both times to 8 subtasks where the policy gave 5. Both were spike runs,
  131 s and 21 s after the spike had ended, with believed backlogs of 35,598 and 3,441 records (the autoscaler's figures for Aggregate; the samples nearest in time show 28,981, and 1,639 falling to 0). In those two runs
  its job was over the limit for 278 and 152 s, the policy's for 190 and 102 s; the first is the run in
  which the LLM job lost a TaskManager (ISSUES.md #129). The LLM's 3.4% more slots with the 4 KiB fetch
  come mostly from the spike (+0.50 slots [0.13, 0.86]) and the slow-down (+0.39 [0.02, 0.77]).
- On the built-in rule's own inputs the policy asked for a scale-up of Aggregate in 98 cycles with the
  default fetch; the rule scaled up in 60 of them, to a smaller size in 24. The policy asked for a
  scale-down in 167 cycles and the rule scaled down in 3 (4 KiB fetch: 58 of 90, 23 smaller; 8 of 191).
- Model calls: decision model 1,697, 0.14 s on average, slowest 1.7 s, $0.008 per job-hour; LLM 1,493,
  2.7 s on average, slowest 12.2 s, $0.044 per job-hour. No fallback. The plugin dropped one proposal of
  the built-in rule (a scale-down), none to scale up.

What this is not. The scenarios are our own and short. Two arms of 40 runs give intervals of 18 to 34
points on the time, so a real difference of 10% could not have been shown: with the spreads seen (49 and
54% per pair for the two models with the default fetch, 32 and 29% with the 4 KiB fetch) an interval
fits inside 10% only from about 100 pairs per arm with the default fetch. The estimates in favour of
the models rest on two scenarios with small times and on two skew runs, and they are larger by day,
when the laptop is in other use.

### Why the models' lead with the default fetch is read as noise (checks of 2026-11-22, after review round 3; none was planned)

`python3 bench/main_marked.py` prints all of these.

- The LLM chose the policy's option in 914 of 914 decisions of the default-fetch arm (852 of 856 with the
  4 KiB fetch), and its pooled figure there is -16.4%. The decision model: 1,010 of 1,019 and 941 of 949.
- Two jobs with the same steps at the same seconds differ by more than the models differ from the
  policy: in run 7 of the skew (default fetch) the policy's job and the decision model's both went from
  4 to 6 Aggregate subtasks at 140 s and to 8 at 230 s (5 s samples), and were over the limit for 88 and
  19 s. The built-in rule's job, with the same steps 5 s earlier, had 100 s.
- In seconds, over the 40 runs of an arm: decision model -4.2 s [-9.4, +0.9] and LLM -4.3 s [-10.2,
  +1.6] with the default fetch; -2.3 s [-8.2, +3.6] and +1.2 s [-8.1, +10.5] with the 4 KiB fetch. On the
  two scenarios with an overload the decision model differs by 0 and +3 s (ramp, spike) and the LLM by
  -6 and +5 s. The pooled percentage comes from the three scenarios at the floor, where a job is over the
  limit for 2 to 29 s on average, most of it the cost of a restart.
- Identical jobs were run in two of the five scenarios (spike, slow-down; E2). There the same statistic
  is -0.3% [-8.3, +7.7] with a spread of 15% per pair (16 runs). The model contrasts have spreads of 29
  to 54%.
- Seconds before the load change: in 8 of the 80 runs a job is over the limit for 5 s or more before the
  first load change (3 of them marked, 7 made by day). With the seconds counted from the load change on,
  the contrasts on the time are -8.0% [-19.9, +3.9], -14.6% [-30.4, +1.2], -14.5% [-31.8, +2.8] with the
  default fetch and -8.9% [-22.2, +4.5], -8.0% [-17.9, +1.8], +0.5% [-8.8, +9.8] with the 4 KiB fetch.
- A marking rule that uses no outcome (warm-up more than 30 s over its plan, or jobs at unequal sizes at
  the start of the measured phase) names 7 runs, all made by day with the 4 KiB fetch; 5 are among the 8
  marked ones, 2 are new (slow-down run 3, skew run 2). Without all ten runs (70 left) the contrasts of
  the 4 KiB arm are -15.9% [-37.7, +5.8], -6.2% [-16.4, +3.9], -3.5% [-12.7, +5.7] on the time, and the
  LLM's slots +4.2% [+0.9, +7.4] (Holm 0.08). No test becomes significant and no wording changes; two
  margin checks differ from those with all runs (the LLM's time with the 4 KiB fetch is outside 10%, the
  decision model's slots with the 4 KiB fetch come inside 4%).
- Agreement by what the policy chose: holds 1,756 of 1,767 (decision model) and 1,568 of 1,570 (LLM);
  scale-ups 124 of 130 and 124 of 125; scale-downs 71 of 71 and 74 of 75. The six applied differences of
  the decision model are scale-ups it did not make in that cycle. In three of the four Aggregate holds
  it made the step 5 to 30 s later than the policy's job did (samples), one cycle after its own hold; so the outcome gaps printed next to those holds above do
  not show an effect of the holds.
- The LLM's two choices above the policy came after most of the gap had built up: before them its job
  stood at 248 and 137 s over the limit and the policy's at 189 and 100 s (59 of the 88 s and 37 of the
  50 s of the final gaps).
- Where the LLM's extra slots come from: in the slow-down its first order came later than the policy's in
  7 of 8 runs of each arm (10.7 s [5.4, 15.9] with the default fetch) and went to 8 subtasks in 9 of 16
  runs, the policy's in 1 of 16, each by the policy's choice on the job's own inputs. Its decision cycle
  is 17.7 s (median of 883 intervals in the measured phases) against 15.1 s for the policy, 15.2 s for
  the decision model and 15.1 s for the built-in rule, so a call on the reconcile thread delays its own
  job's decisions and not the others'.
- The delay of a scenario's changes and the hot keys of the skew are drawn per scenario and repetition
  number, so the two arms of the campaign share the same eight draws. The order of the jobs is the same in
  37 of the 40 pairs of runs.
- Correction of 2026-11-22 10:20 PST (audit 8): one of the eight marked runs has its task failures inside
  the load change. In the ramp of the 4 KiB arm, repetition 4, the ramp runs from 24 to 265 s of the
  measured phase and the built-in rule's job and the policy's job lost a task at 66 and 76 s.
  `bench/job_exceptions.py` printed "42 s after it ended" for it, where 42 s is counted from the start of
  the ramp (ISSUES.md #134). By the rule of 18 Nov the run is made again alone in the night of 22 Nov; the
  original is in `results-v3-first/accurate/ramp/rep4`. Every number of this section includes the
  original run until then. With all runs that run alone carries -72 s of decision model minus policy,
  3.4 of the 6.4 points of that arm's pooled -6.4%.

---

The findings of the pilot runs (first version of the plugin and benchmark, 2026-11-16 and 17) are in
[FINDINGS-pilot.md](FINDINGS-pilot.md). They are superseded and not valid for comparing controllers.

## 15. A cap on the trend projection keeps the policy from undoing its scale-down when a load returns, but two of five runs still undid a later, deeper scale-down (cluster test B, scale 2, 5 runs)

Section 10 showed the plugin's policy running out of phase with a load that falls and comes back within
minutes: it scaled down in the low phase and up again when the load returned, with or without a look-back
of ten minutes. The look-back alone did not help because the first reading after a restart is compared
with the last one before it, and the trend projection then doubles the demand (`ISSUES.md` #119). The
change tested here (`scale-down.window-caps-trend`, operator image `cluster10`): while the demand is not
more than 10% above the highest demand the look-back window saw more than 90 s ago, the projection is at
most the higher of that level and the demand.

Test, registered before the runs (`paper/outline.md`, 21 Nov 16:53 and 22 Nov 01:26 PST): the on-off load
of section 10 (800 and 3,200 records/s at scale 2, two minutes each), five jobs side by side, five runs
on 23 Nov between 04:53 and 07:22 UTC. `builtin+max70`; `rule`, the policy; `rule+b`, the policy with the
look-back; `rule+c`, look-back, cap, and a scale-down has to free two slots; `llm+c`, the LLM with the
history in its prompt and the same two-slot rule. No run was invalid, and none was disturbed by the
cluster (`cluster/bench/disturbed_cluster.py`, run on each before its numbers were read).

| from the second high phase on | `builtin+max70` | `rule` | `rule+b` | `rule+c` | `llm+c` |
|---|---|---|---|---|---|
| restarts per run | 4, 3, 3, 4, 3 | 7, 7, 8, 7, 7 | 8, 7, 8, 7, 7 | 1, 4, 2, 2, 2 | 4, 5, 6, 4, 3 |
| scale-downs undone within 5 min, mean | 0.6 | 2.8 | 3.0 | 0.2 | 1.8 |
| seconds over the limit, mean (harness) | 24.2 | 30.0 | 25.2 | 6.4 | 13.8 |
| whole run: seconds over the limit, mean (monitoring service) | 155.0 | 154.8 | 151.2 | 138.0 | 137.6 |
| whole run: slots in use, mean (monitoring service) | 11.88 | 13.15 | 13.35 | 11.58 | 13.59 |

Against the predictions: the projection check held in all 329 evaluations with a demand within the
margin (B2; without the cap it fails in 36 of 176); the time over the limit was 13 s below the look-back's
on average, where the plan allowed 20 s above (B3). Not met: "at most 3 restarts in every run" (4 in run
2, B1); "at least as many slots as the policy" (it used 1.6 fewer in every run, so the cost the plan
expected did not appear, B4); and "no scale-up at a demand within the margin" (two, B5). Two such
scale-ups are more than the plan allowed, so by the rule fixed in advance the test counts against the
change as a complete repair.

The two scale-ups have another cause than the projection. In runs 2, 4 and 5 `rule+c`, holding 12
subtasks of Aggregate, made a second scale-down to 10 late in the run. At those evaluations the capacity
estimate of one subtask was 460 to 463 records/s, and the earlier level divided by the target
utilization and that capacity came to 9.97, 10.00 and 9.99. When the load returned the estimate was 443
and 432, the same division gave 10.4 and 10.7, and the policy went to 15 (runs 2 and 4; run 5 ended
first). The estimate at 12 subtasks ranged from 424 to 474 over the runs. A scale-down that is kept for
an earlier level is sized with one reading of a capacity that moves by 5%, and nothing separates the
size it picks from the size at which the policy scales up again.

Limits: five runs, one load shape, one cluster, no significance test. The remedy for the second cause
(a margin on the size a scale-down keeps for an earlier level) was built and tested afterwards (below).

Reproduce: `python3 cluster/bench/flapsum.py results-cluster/policyfix2/onoff/rep*`,
`python3 cluster/bench/flaporders.py results-cluster/policyfix2/onoff/rep*`, monitoring extracts in
`results-cluster/policyfix2/datadog.json`.

### The margin on the size a scale-down keeps (Test D, 5 runs on 23 Nov, planned, after the paper's data cut-off)

Plan entry of 23 Nov 00:31 PST, checks and outcome entries of 14:45 and 14:48 PST. The same on-off load,
five jobs side by side, operator image `cluster11`: the policy without a look-back (`rule`), with the
look-back, the cap and the two-slot rule as above (`rule+c`), the same plus the margin (`rule+d`:
a scale-down keeps what the window's highest demand plus 10% needs), the LLM with the look-back, and the
built-in rule. All five runs are clean.

From the second high phase on:

| Job | restarts per run | scale-downs undone | s over the limit |
|---|---|---|---|
| policy, no look-back | 7, 8, 7, 7, 7 | 2.8 per run | 29, 124, 22, 31, 45 |
| look-back, cap, two slots | 1, 2, 1, 1, 2 | one, ordered 8 s before the end of run 2 | 2, 3, 2, 1, 24 |
| the same plus the margin | 1, 2, 2, 1, 2 | none | 3, 21, 4, 3, 13 |
| LLM with the look-back | 4, 5, 3, 4, 2 | 1.6 per run | 39, 14, 17, 11, 14 |
| built-in rule, two-minute scale-down delay | 0, 4, 2, 0, 4 | 0.6 per run | 0, 13, 9, 0, 2 |

Four of five predictions held: at most two restarts of the job with the margin in every run; the replay of
the policy with the margin gives its logged choice in 295 of 295 evaluations; it ordered no scale-up at a
demand within the margin of the earlier level; it was over the limit for 2.4 s more than the job without
the margin (10 s was the bound). The fifth failed on its size: the margin cost 0.05 slots [-0.28, 0.37]
where 0.3 to 1.5 was predicted. The clause against the change is not met.

The case the margin is for occurred once. In run 2 the job without it went from 12 to 10 subtasks at
1,039 s, because the earlier level needed 9.93 subtasks at that evaluation's capacity reading, and
ordered 10 to 15 at 1,432 s, 88 s into a high phase, at a demand within the margin of that level. The job
with the margin stayed at 12 in the same run: at five evaluations the form without the margin would have
scaled down, and with it the level needed 11.1.

With Test B: the job without the margin undid a scale-down in three of ten runs, the job with it in none
of five. That is not a test, and one event in five control runs is thin. What the two tests show together
is that the look-back with the cap, the two-slot rule and the margin takes the policy from 7 to 8
restarts per run on a load that comes back every two minutes to 1 or 2, with no scale-down undone and
without more time over the limit than the built-in rule (132 s against 144 s over the whole run, 11.4
against 11.3 slots).

Reproduce: `python3 cluster/bench/flapsum.py results-cluster/keepfix/onoff/rep*`,
`python3 cluster/bench/flaporders.py results-cluster/keepfix/onoff/rep*`,
`python3 cluster/bench/keepreplay.py --job d results-cluster/keepfix/onoff/rep*` (and without `--job d` for
the job without the margin), monitoring extracts in `results-cluster/keepfix/datadog.json`.


## 16. With five-minute windows a demand that follows a jump in the load cut the built-in rule's time over the limit from 20 minutes to two and a half (cluster test C, scale 2, 6 runs)

Section 13 ended with a cost of the broker-side signal: the built-in rule sees the true backlog a few
seconds after the load rises, adds it to a demand that is still the average of a window of old load,
and orders a small first step. With five-minute windows every job did that and reached its full size
only 450 to 507 s after the rise (`ISSUES.md` #123). The change tested here (`lag.step-aware`, operator
image `cluster10`): when the records of the newest interval between two evaluations arrived more than 20%
faster than the window's average and the backlog grew by more than that share, the source's demand is
the newest interval's rate, or the rate between two readings of the end offsets within the same cycle
when that is higher. The demand is never lowered.

Test, registered before the runs (`paper/outline.md`, 21 Nov 21:31 PST): 400, then 4,800 records/s for 40
minutes at scale 2, five-minute metric window, four jobs side by side with the broker-side signal: the
built-in rule with the shipped band and with the band at the target, each without and with the option.
Six runs on 23 Nov, measured windows from 07:32 to 13:14 UTC. No run was invalid, no broker read
failed, and none was disturbed by the cluster (`cluster/bench/disturbed_cluster.py`, run before any number
was read).

| per job-run, 6 runs | shipped band | shipped band, option | band at target | band at target, option |
|---|---|---|---|---|
| first order for Aggregate after the rise | 3 or 4 | 12 to 16 | 3 | 12 to 16 |
| seconds from the rise to that order | 24 to 37 | 0.6 to 15.3 | 13 to 24 | 2.5 to 15.6 |
| seconds until 10 subtasks run | 492 to 520 | 44 to 62 | 483 to 510 | 45 to 62 |
| scale-ups of Aggregate | 2 | 1 | 2 | 1 |
| seconds over the limit, mean (monitoring service) | 1,224 | 145 | 1,262 | 145 |
| slots in use, mean (monitoring service) | 13.27 | 15.20 | 13.33 | 15.20 |

All seven predictions held. The primary one: with the shipped band the job with the option was over
the limit for fewer seconds in 6 of 6 runs, by 1,049 to 1,104 s (mean -1,078.8 [-1,099.9, -1,057.8];
exact sign-flip p 0.031, the smallest six pairs can give). With the band at the target: 6 of 6, mean
-1,117.5. Step lines appeared only on jobs with the option, the last 103 s after the rise. The cost is
1.9 slots on average over the 48-minute window: the job is at 16 subtasks about seven and a half
minutes sooner.

A weakness outside the predictions (`ISSUES.md` #141). The reading within a cycle spans about half a
second. At the first step line of a job-run it did its work: the demand was 0.74 to 1.15 of the load,
where the newest interval alone was below 0.9 of it in 9 of 12. At the 31 later lines the newest
interval was within 3% of the load every time, and the reading within the cycle ranged from 0.40 to
2.26 of it. In run 3 one job took a demand of 10,830 and 10,637 records/s against a load of 4,800.
Aggregate was already at the job's maximum of 16, so the only effect was six subtasks instead of three
for the operator after it and one more restart when that was undone. On a job with more room the order
would have been about twice too large. Using the reading within the cycle at the first step line of a
rise only removes all three cases in a recomputation from the 43 logged lines; that rule is not built.

Limits: six runs, one load shape (a single rise by a factor of 12), one cluster, and a maximum of 16
subtasks that is close to what the load needs. During the warm-up and the first 24 s of run 4, more than
two minutes before its rise, files were sent to the driver pod; the run is kept and lies inside the
range of the other five.

### The same option with one-minute windows (cluster test A, 10 runs, registered on 21 Nov)

400, then 4,800 records/s for 8 minutes, the same four jobs, ten runs on 23 Nov between 13:19 and 16:01
UTC, all valid and none disturbed. With one-minute windows the rule without the option usually recovers
from a small first order by itself: the order needs new TaskManagers and the next evaluation raises it
to 16 before the job has restarted. That happened in 9 of 10 runs; in one run (first evaluation 6 s after
the rise) the step from 2 to 4 was carried out and the job reached 16 only after 150 s.

| 10 runs | shipped band | shipped band, option | band at target | band at target, option |
|---|---|---|---|---|
| first order for Aggregate | 4 to 10 | 16 | 4 to 10 | 16 |
| seconds until 10 subtasks run, mean | 77 | 47 | 77 | 45 |
| seconds over the limit, mean (monitoring service) | 208 | 136 | 207 | 128 |
| slots in use, mean | 13.08 | 13.47 | 13.08 | 13.48 |

With the option minus without: -72.1 s [-116.7, -27.5] and -79.6 s [-123.2, -36.0], lower in 10 of 10
pairs each, for 0.39 slots. In the run with the carried-out small step: -241 and -239 s. Predictions A1,
A2 and A5 held. A3 and A4 held in part: they named the pairs in which the job without the option first
ordered fewer than 8, four pairs, and asked for 60 s sooner and 100 s less there; that held in the two
pairs of the run above and not in two pairs of another run, where the small order was replaced before
it was carried out (20 and 30 s sooner, 56 and 82 s less). No against-clause is met.

The weakness of section 16 shows here at the first step line: the reading within the cycle was 1.4 to
2.6 times the load at 5 of 20 first lines. Aggregate was ordered to its maximum of 16 in every job-run,
so nothing followed for it; the operator behind it was ordered to 5 to 7 subtasks in four job-runs and
the next evaluation corrected that to 4 before the job restarted. On a job below its maximum the first
order would have been too large by that factor (`ISSUES.md` #141).

Reproduce: `python3 cluster/bench/stepscore.py results-cluster/stepfix surge_long`,
`python3 cluster/bench/scalesteps.py results-cluster/stepfix/surge_long/rep*`,
`python3 cluster/bench/stepnow.py results-cluster/stepfix/surge_long/rep*` (the step lines against the load),
the same three with `surge` for test A, monitoring extracts in `results-cluster/stepfix/datadog.json`.

### Where the wrong half-second readings come from (a probe on 23 Nov, after the end of the paper's data collection)

`cluster/bench/offprobe.py` creates one topic, sends to it at a known rate with the harness's producer
settings, and reads the end offsets at about 0, 0.45, 1 and 2 s, without Flink and without the operator.
It stamps the readings in three ways: when the answer is in (the plugin in Tests C and A), at the middle
between request and answer (the plugin since 23 Nov), and per partition leader. On the cluster it ran
for four minutes right after the last run of the default-window campaign (16 partitions, three
replicas, three jumps from 400 to 4,800 records/s; `results-cluster/offprobe/cluster-2026-11-23.csv`).

| Load 4,800 records/s, steady (36 readings) | lowest | highest | within 20% of the load |
|---|---|---|---|
| rate over 0.45 s | 0.26 to 0.30 | 1.88 to 2.03 | 83 to 86% |
| rate over 1 s | 0.53 to 0.54 | 1.43 to 1.46 | 81 to 86% |
| rate over 2 s | 0.79 to 0.84 | 1.22 to 1.23 | 94 to 97% |

The ranges are over the three ways of stamping, which differ little; a listing took 30 ms (median) and
157 ms at most. At 400 records/s the half-second rate ranged from 0.4 to 3.2 times the load and the
two-second rate from 0.95 to 1.29. On the laptop (one broker, no replication, the same script) every
half-second reading was 0.93 to 1.12 of the load and every two-second reading 0.97 to 1.03.

Reading. The error is in what the brokers return, not in the operator's timing and not in the load
generator: a listing of the latest offsets returns the high watermark, which on a replicated topic
waits for the followers and so advances unevenly at the scale of half a second. That matches what Tests
C and A showed (most readings right, some far off, two jobs off in the same cycle). A span of two
seconds, which the changed plugin uses at the first step, brings the reading within about 20% of the
load. It does not make it exact. The run in closed loop is the next subsection.

### The remedy in closed loop (cluster Test E, five runs, registered on 23 Nov 08:20 PST; not for the paper)

Operator image `cluster12`: later step lines take the newest interval's rate alone; at the first step line
of a rise, when the half-second reading is above the newest interval's rate, the end offsets are read a
third time two seconds after the first reading, and that rate is the demand if it is above the newest
interval's rate. Scenario `surge` at scale 2 with one-minute windows, as Test A, four jobs side by side
that all have the broker-side signal and the option (shipped band and band at the target, two of each).
Five runs on 23 Nov between 14:46 and 16:36 PST, all clean. Run 5 was made twice: the cluster evicted the
driver pod during the first attempt and nothing of it exists (`ISSUES.md` #151). The test began after the
paper's data cut-off and is not in the paper. Scripts: `cluster/bench/stepnow.py` (step lines) and
`cluster/bench/escore.py` (orders and times); every count below takes the load at the line's own moment.

| Prediction | Outcome |
|---|---|
| E1. Demand 0.8 to 1.25 of the load at every first step line with a third reading | Not met: 14 of 18 (0.95 to 1.15); the four jobs of run 5 got 0.68 to 0.76 |
| E2. The half-second reading is outside 0.8 to 1.25 at 15% or more of the first step lines | Not met: 2 of 21 (1.55 and 1.39 times the load); Test A had 5 of 20 |
| E3. The first order asks for 10 subtasks or more in every job-run | Met: 14 to 16 in 20 of 20, 2 to 18 s after the rise |
| E4. At every later step line the demand is the newest interval's rate, 0.9 to 1.1 of the load | Met: 34 of 34, 0.97 to 1.02 |
| E5. Ten subtasks run within 75 s in every job-run | Not met by one second: 19 of 20 (38 to 76 s) |
| Against the remedy: a demand above 1.4 times the load after a third reading; a first order below 10 in more than one job-run; more than 100 s to ten subtasks | None of the three |

What the lines show.
- The over-reading is gone. No step line of this build put the demand above 1.15 times the load. The one
  half-second reading of the tested rise that was far off (7,436 records/s against 4,800, run 3) was
  replaced by a two-second reading of 5,502. At the 34 later lines the half-second reading, which this
  build no longer uses there, was 0.45 to 1.22 of the load and outside 0.8 to 1.2 at six.
- A new error in the other direction. In run 5 all four jobs took their third reading about 10 s after
  the rise and got 3,277 to 3,650 records/s over two seconds, 0.68 to 0.76 of the load, while their
  half-second readings were right (0.99 to 1.07) and the generator's log shows the full rate. The newest
  interval still held part of the time before the rise (0.57 to 0.60 of the load), so the low two-second
  reading was above it and became the demand. The probe had seen 0.79 to 1.23 over two seconds; this is
  outside that range, on four topics at once.
- It cost little here. The first orders of run 5 were 2 to 15, 15, 15 and 14 subtasks instead of 16, and
  ten are needed. Ten subtasks ran 56 to 76 s after the rise in run 5 and 38 to 64 s in the other runs,
  but run 5 also began 16 minutes after the node replacement, so the longer times cannot be put on the
  reading.
- Seconds over the limit (monitoring service): 155 s on average over the 20 job-runs (111 to 213 s; by
  run 171, 117, 166, 144, 181); slots held 13.7. The plan has no contrast for these.

Reading. Two of five predictions met, none of the clauses against the change. The change removes what it
was made to remove, and the runs exercised that case once, so they say little about how well. They also
show that a longer span is not a safe reading either: the brokers' end offsets can stall for a good part
of two seconds. A reading that cannot be far off in either direction would need a signal that does not
depend on the high watermark, or a rule that waits one more evaluation when the two readings disagree.
Neither is built. `ISSUES.md` #141 stays open.

## 17. The last laptop night: the main campaign with its replacement run, identical jobs in three more scenarios, held-out workloads, slower restarts, failing model calls, single jobs (23 Nov)

All of it was read after the checks entry of 23 Nov 07:11 PST (`paper/outline.md`): 38 runs, none disturbed
by another container, no testbed event. The four repeats of E8 and the further held-out arms were not
made. One run that started three minutes after the end of data collection is set aside and not read
(`ISSUES.md` #142).

### Main campaign with the replacement run (section 14 with one run exchanged)

The ramp with the 4 KiB fetch, repetition 4, had two task failures inside its ramp and was made again
on the evening of 22 Nov. The arm with the default fetch is unchanged. 4 KiB fetch, 40 runs:

| contrast | time over the limit | with the original run | slots |
|---|---|---|---|
| PaC - Builtin | -6.8% [-22.7, +9.0] | -4.8% [-19.9, +10.4] | -2.9% [-9.3, +3.6] |
| DM - PaC | -2.2% [-10.1, +5.7] | -6.4% [-16.6, +3.9] | +1.1% [-2.2, +4.5] |
| LLM - PaC | +1.2% [-8.4, +10.7] | -0.5% [-9.7, +8.6] | +3.4% [+0.5, +6.2] |

No test is significant after Holm in either arm, as before, and the wording stays "no detectable
difference"; the intervals now allow up to 34% less or 11% more. One margin check changes: the LLM's
time with the 4 KiB fetch is no longer inside 10%. On the ramp with the 4 KiB fetch the policy is now
21.2 s [41.9, 0.6] ahead of the built-in rule, in 7 of 8 runs. Decisions of the measured phases: decision
model 1,946 of 1,963, LLM 1,763 of 1,767. The replacement run ended while the host's load rose (38, 34
and 25 in its last two and a half minutes), so it stays in the list of marked runs, and the figures
without marked runs are those of section 14.

### Identical jobs in the skew, the ramp and the drop (E2c, 24 runs, four predictions, all met)

Four jobs with the policy in code and identical settings, the same eight draws as the main campaign.
The statistic of the main campaign's contrasts applied to two identical jobs:

| | pooled difference | spread per pair | skew | ramp | drop |
|---|---|---|---|---|---|
| job b minus job a | +4.6% [-27.3, +36.6] | 76% | 132% | 35% | 5% |
| job d minus job c | -4.3% [-16.5, +8.0] | 29% | 46% | 19% | 10% |
| DM minus PaC, main campaign, same scenarios | -19.1% [-44.9, +6.7] | 61% | | | |
| LLM minus PaC, main campaign, same scenarios | -21.9% [-50.0, +6.1] | 66% | | | |

In the skew two identical jobs differed by up to 90 s in one run. Over all six pairs of a run the mean
absolute relative difference is 26.5% [9.5, 43.6] (skew 52%, ramp 22%, drop 6%), against 10% in the
spike and the slow-down (section 4). So identical jobs in these scenarios differ as much as the models
differed from the policy, which is what the paper's reading of the models' lead as noise needed. The
models' pooled means (-19% and -22%) lie inside the interval of one identical pair and below that of the
other; this is a comparison of spreads, not a test.

### Ten held-out workloads (E5, one run each, two predictions, both met)

Workloads drawn at random after the plugin, the prompts and the thresholds were frozen (seeds 101 to
110). The decision model's most likely option was the policy's in 1,004 of 1,024 decisions of the
measured phases (98.0%; 20 below, 2 applied) and the LLM's in 869 of 871 (99.8%; 2 above, both applied).
Pooled over the workloads: DM - PaC +7% [-8, +21] of a workload's mean time over the limit and +1%
[-2, +4] of the slots; LLM - PaC +3% [-14, +19] and +1% [-1, +4]; PaC - Builtin -17% [-49, +15] and +1%
[-3, +5]. Ten pairs cannot put an interval inside 10%. No fallback in 1,312 model calls.

### Restarts that take a minute longer (E4, 10 runs, no prediction): the policy was over the limit for longer, mostly because the built-in rule kept the size it had reached in the warm-up

Every restart waits 60 s longer when the job restores, and the controllers are told a restart time of
70 s. A longer spike and a longer ramp, five runs each.

| | Builtin | PaC | DM | LLM |
|---|---|---|---|---|
| spike: seconds over the limit / rescalings | 175 / 2.0 | 306 / 3.4 | 263 / 2.8 | 292 / 3.0 |
| ramp: seconds over the limit / rescalings | 244 / 1.6 | 350 / 2.8 | 357 / 2.8 | 369 / 2.8 |

PaC - Builtin: +44.9% [+20.9, +69.0] of a scenario's mean time over the limit, higher in 9 of 10 runs
(+131 s in the spike, +105 s on the ramp), slots -1.7% [-13.4, +10.1]. In the six runs made after the
other project's workload had left the laptop: +64.2% [+31.6, +96.7]. DM - PaC -6.6% [-16.6, +3.4], LLM -
PaC +0.2% [-7.8, +8.3]. With ordinary restarts the policy was 27 and 28 s ahead of the built-in rule in
the short spike and ramp (section 14).

Correction of 23 Nov 11:04 PST (review round 4, methodology review, W1; checked with
`python3 bench/e4_start.py`). The first reading of these runs was "the policy takes more steps and pays
for each restart". The runs do not isolate that. With every restart 60 s longer the first start of a job
is slow as well, a backlog builds in the warm-up, and 28 of the 40 job-runs scaled Aggregate up before
the measured phase began; in none of the ten runs were all four jobs at the scenario's size at its
start. The built-in rule kept that size, with its scale-down delay, and met the load change above the
scenario's size in 6 of the 10 runs. The policy gave the capacity back before the load changed in 7 of
the 10, which cost it a restart: it was over the limit for 32.3 s per run before the load changed (the
built-in rule: 0.6 s). Split by the built-in rule's size just before the load change, a state that uses
no outcome:

| Runs | PaC - Builtin, share of a scenario's mean time over the limit | PaC longer in |
|---|---|---|
| all ten | +44.9% [+20.9, +69.0] | 9 of 10 |
| Builtin at the scenario's size at the load change (4) | +15.0% [-10.9, +40.9] | 3 of 4 |
| Builtin above it (6) | +64.9% [+37.6, +92.2] | 6 of 6 |
| all ten, from the load change on | +32.0% [+8.7, +55.2] | 7 of 10 |

So where both jobs met the load change at the same size, the runs show no detectable difference, and
four runs are too few to say there is none. What the campaign does show: with slow restarts a job that
gives capacity back pays a restart for it and is small when the load comes, and a job that holds its
size is ready. That is the scale-down delay again (section 10), not a measurement of what extra steps
cost. The harness's warm-up check waits for the backlog to drain and does not look at the size; by the
main campaign's rule for a disturbed start (jobs at unequal sizes) eight of the ten runs would be marked
(`ISSUES.md` #146). The paper no longer carries this result in its abstract.

### Model calls that fail or hang during the spike (E9, three runs each)

In 47 of the 48 cycles that fell into the fault the plugin fell back to the built-in rule (decision
model 17 of 17 and 11 of 11, LLM 13 of 14 and 6 of 6). The first scale-up came 52 and 50 s after the
spike began with failing calls and 74 and 87 s with hanging calls (policy 12 and 14 s, built-in rule 25
and 29 s); a hanging call stretched a cycle from 15 s to 27 and 31 s. Seconds over the limit: 154 and
173 (failing), 173 and 160 (hanging), where the same controllers take 145 and 147 s without a fault and
the built-in rule took 175 and 168 s in the same runs. During the last two and a half minutes of one
hanging run the host's load was 33 to 70; without that run the figures are 172 and 155 s. Six runs, no
test.

### One job alone (E2, spike, three runs each)

Built-in rule alone 172.7 s over the limit, among four 168.8 s (five runs): +3.9 s [-15.6, +23.3]. Policy
alone 149.3 s, among four 136.5 s (three runs): +12.8 s [-5.6, +31.3]. Both together +8.4 s [+0.4,
+16.3], six runs against eight that were made five days earlier. Running four jobs side by side did not
lengthen a job's time over the limit; six runs cannot show an interference below about 10 s.

Reproduce: `python3 bench/paper.py main` (and `e2c`, `e5`, `e4`, `e9`, `e2`), `python3 bench/main_marked.py`,
`python3 bench/e2c_pairs.py`, `python3 bench/e4_blocks.py`, `python3 cluster/bench/decount.py <run dirs>`.
