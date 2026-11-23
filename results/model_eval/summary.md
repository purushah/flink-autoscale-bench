# Model check against the built-in formula

Settings: target_utilization_pct = 60, min_parallelism = 1, max_parallelism = 8, backlog_catch_up_seconds = 60.

Each cell shows the answers given and how often, e.g. `5 ×9, 4 ×1`. **Bold** cells agree with the formula on every repeat.

| Case | Current | Formula | Jev, raw inputs | Jev, computed options | Claude Haiku 5.5 | Claude Sonnet 5.5 | Claude Sonnet 5.5, high effort | Claude Opus 5.5 | Claude Fable 5.2 |
|---|---|---|---|---|---|---|---|---|---|
| Steady, on target | 4 | 4 | **4 ×10** | **4 ×10** | **4 ×5** | 5 ×3, 7 ×2 | 5 ×3, 7 ×2 | **4 ×5** | **4 ×5** |
| Clear overload | 2 | 5 | 3 ×10 | **5 ×10** | **5 ×5** | **5 ×5** | **5 ×5** | **5 ×5** | **5 ×5** |
| Slightly over target | 4 | 5 | 4 ×10 | **5 ×10** | **5 ×5** | 6 ×3, 5 ×2 | **5 ×5** | **5 ×5** | **5 ×5** |
| Clearly oversized | 8 | 2 | 1 ×10 | **2 ×10** | **2 ×5** | 8 ×5 | 8 ×3, error ×1, 7 ×1 | **2 ×5** | **2 ×5** |
| Slightly oversized | 5 | 5 | **5 ×10** | **5 ×10** | **5 ×5** | 5 ×4, 8 ×1 | **5 ×5** | **5 ×5** | **5 ×5** |
| Over target with backlog | 4 | 7 | 8 ×10 | **7 ×10** | **7 ×5** | 8 ×5 | 8 ×5 | **7 ×5** | **7 ×5** |
| Demand beyond the limit | 4 | 8 | **8 ×10** | **8 ×10** | **8 ×5** | **8 ×5** | **8 ×5** | **8 ×5** | **8 ×5** |
| Worker capacity halved | 4 | 6 | 4 ×10 | **6 ×10** | **6 ×5** | **6 ×5** | **6 ×5** | **6 ×5** | **6 ×5** |
| Rounding boundary, 900 rec/s | 2 | 3 | 3 ×9, 2 ×1 | **3 ×10** | **3 ×5** | 4 ×5 | 4 ×5 | **3 ×5** | **3 ×5** |
| Rounding boundary, 901 rec/s | 2 | 4 | 3 ×9, 2 ×1 | **4 ×10** | **4 ×5** | 4 ×4, error ×1 | **4 ×5** | **4 ×5** | **4 ×5** |
| Skew (not scored) | 4 | 4 (not scored) | 4 ×10 | 4 ×10 | 4 ×5 | 8 ×3, 4 ×1, error ×1 | 4 ×5 | 4 ×5 | 4 ×5 |

## Scores over the 10 scored cases

| Model | Answers | Exact | Within 1 | Too low (leaves vertex above target) | Too high | Errors | Cases answered the same on every repeat | Median latency | Cost per decision |
|---|---|---|---|---|---|---|---|---|---|
| Jev, raw inputs | 100 | 39 (39%) | 79 (79%) | 51 | 10 | 0 | 8 of 10 | 214 ms | $0.000031 |
| Jev, computed options | 100 | 100 (100%) | 100 (100%) | 0 | 0 | 0 | 10 of 10 | 189 ms | $0.000035 |
| Claude Haiku 5.5 | 50 | 50 (100%) | 50 (100%) | 0 | 0 | 0 | 10 of 10 | 1,507 ms | $0.000154 |
| Claude Sonnet 5.5 | 50 | 25 (50%) | 41 (82%) | 0 | 24 | 1 | 6 of 10 | 1,612 ms | $0.004640 |
| Claude Sonnet 5.5, high effort | 50 | 30 (60%) | 43 (86%) | 0 | 19 | 1 | 8 of 10 | 1,988 ms | $0.004935 |
| Claude Opus 5.5 | 50 | 50 (100%) | 50 (100%) | 0 | 0 | 0 | 10 of 10 | 1,951 ms | $0.004156 |
| Claude Fable 5.2 | 50 | 50 (100%) | 50 (100%) | 0 | 0 | 0 | 10 of 10 | 2,362 ms | $0.006086 |
