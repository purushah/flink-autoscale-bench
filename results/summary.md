# Benchmark summary

Each cell is the mean over repetitions, with the lowest and highest repetition in brackets.

## ramp (3 repetitions)

| Metric | Built-in | Jev |
|---|---|---|
| p50 latency (ms) | 179 (167–194) | 133 (132–133) |
| p95 latency (ms) | 24,980 (16,421–32,246) | 29,859 (28,400–32,168) |
| p99 latency (ms) | 32,473 (20,981–40,930) | 36,899 (34,738–39,719) |
| seconds with p95 > 2000 ms | 86.3 (68.0–103) | 53.0 (50.0–55.0) |
| recovery time (s) | 268 (171–323) | 300 (267–360) |
| throughput (rec/s) | 1,343 (1,321–1,354) | 1,395 (1,380–1,423) |
| largest backlog (records) | 45,013 (32,630–56,467) | 43,083 (40,460–45,683) |
| average backlog (records) | 9,060 (7,539–10,395) | 8,927 (8,370–9,990) |
| average total parallelism | 6.34 (6.12–6.49) | 10.3 (9.88–10.7) |
| peak Aggregate parallelism | 7 (5–8) | 8 (8–8) |
| average TaskManager pods | 1.38 (1.24–1.67) | 1.59 (1.56–1.64) |
| scaling actions | 2.67 (2–3) | 3.33 (3–4) |
| direction reversals | 0 (0–0) | 0 (0–0) |

Jev model use per run: 11.3 calls, 24,806 input and 3,270 output tokens, $0.00104 ($0.0069 per hour), mean call latency 119 ms, 0 fallbacks in total.

## spike (3 repetitions)

| Metric | Built-in | Jev |
|---|---|---|
| p50 latency (ms) | 33,411 (33,021–33,607) | 11,328 (7,377–14,041) |
| p95 latency (ms) | 93,502 (81,762–103,373) | 45,874 (30,607–56,698) |
| p99 latency (ms) | 107,587 (92,506–122,467) | 55,893 (38,331–66,380) |
| seconds with p95 > 2000 ms | 187 (179–198) | 142 (131–153) |
| recovery time (s) | 242 (179–360) | 258 (230–277) |
| throughput (rec/s) | 903 (895–915) | 907 (885–931) |
| largest backlog (records) | 133,817 (126,167–144,225) | 87,916 (74,869–95,455) |
| average backlog (records) | 32,783 (29,192–35,991) | 16,492 (13,368–19,052) |
| average total parallelism | 7.51 (6.11–8.49) | 9.73 (8.86–10.6) |
| peak Aggregate parallelism | 7 (5–8) | 7.33 (6–8) |
| average TaskManager pods | 1.69 (1.67–1.71) | 1.67 (1.58–1.82) |
| scaling actions | 3 (3–3) | 3 (3–3) |
| direction reversals | 0.33 (0–1) | 1 (1–1) |

Jev model use per run: 13.0 calls, 27,517 input and 3,542 output tokens, $0.00116 ($0.0069 per hour), mean call latency 119 ms, 0 fallbacks in total.

## scale_down (3 repetitions)

| Metric | Built-in | Jev |
|---|---|---|
| p50 latency (ms) | 134 (126–139) | 8,217 (139–24,040) |
| p95 latency (ms) | 3,565 (205–10,283) | 43,363 (21,568–68,010) |
| p99 latency (ms) | 8,043 (3,306–12,768) | 52,685 (29,597–76,499) |
| seconds with p95 > 2000 ms | 6 (3–12.0) | 48.3 (27.0–74.0) |
| recovery time (s) | 200 (98.0–300) | 123 (115–130) |
| throughput (rec/s) | 575 (557–603) | 731 (654–818) |
| largest backlog (records) | 18,399 (13,663–21,505) | 83,183 (66,964–114,946) |
| average backlog (records) | 2,895 (2,471–3,289) | 10,276 (5,970–17,385) |
| average total parallelism | 11.2 (9.47–12.2) | 11.9 (10.9–12.8) |
| peak Aggregate parallelism | 8 (8–8) | 8 (8–8) |
| average TaskManager pods | 2 (2–2) | 1.84 (1.76–2) |
| scaling actions | 1.67 (1–2) | 2 (2–2) |
| direction reversals | 0.33 (0–1) | 0.33 (0–1) |

Jev model use per run: 15.0 calls, 31,181 input and 3,852 output tokens, $0.00131 ($0.0075 per hour), mean call latency 151 ms, 0 fallbacks in total.

## skew (3 repetitions)

| Metric | Built-in | Jev |
|---|---|---|
| p50 latency (ms) | 140 (136–142) | 164 (133–226) |
| p95 latency (ms) | 2,147 (209–3,547) | 23,097 (5,677–33,234) |
| p99 latency (ms) | 8,388 (2,959–11,770) | 29,291 (9,037–39,737) |
| seconds with p95 > 2000 ms | 10.3 (2–15.0) | 82.3 (22.0–154) |
| recovery time (s) | 102 (85.0–127) | 164 (111–255) |
| throughput (rec/s) | 913 (903–918) | 958 (914–984) |
| largest backlog (records) | 11,903 (6,838–16,173) | 32,060 (10,769–46,612) |
| average backlog (records) | 4,678 (4,073–5,798) | 7,846 (4,610–9,992) |
| average total parallelism | 9.79 (9.57–10.2) | 10.8 (9.49–12.4) |
| peak Aggregate parallelism | 8 (8–8) | 8 (8–8) |
| average TaskManager pods | 2 (2–2) | 1.9 (1.82–1.94) |
| scaling actions | 1 (1–1) | 2.33 (2–3) |
| direction reversals | 0 (0–0) | 0 (0–0) |

Jev model use per run: 12.0 calls, 23,589 input and 3,041 output tokens, $0.00099 ($0.0066 per hour), mean call latency 149 ms, 0 fallbacks in total.

## capacity (3 repetitions)

| Metric | Built-in | Jev |
|---|---|---|
| p50 latency (ms) | 142 (135–149) | 845 (138–1,296) |
| p95 latency (ms) | 538 (225–745) | 18,454 (6,786–25,665) |
| p99 latency (ms) | 11,270 (10,651–12,089) | 23,662 (11,534–30,974) |
| seconds with p95 > 2000 ms | 7 (6–8) | 143 (28.0–202) |
| recovery time (s) | 128 (34.0–266) | 299 (223–360) |
| throughput (rec/s) | 815 (812–820) | 831 (826–835) |
| largest backlog (records) | 12,154 (9,139–14,254) | 20,740 (10,848–27,076) |
| average backlog (records) | 4,491 (3,503–5,360) | 6,768 (4,262–8,347) |
| average total parallelism | 8.43 (6–9.67) | 8.95 (8.3–9.52) |
| peak Aggregate parallelism | 6.67 (4–8) | 7 (5–8) |
| average TaskManager pods | 1.67 (1–2) | 1.51 (1.44–1.64) |
| scaling actions | 1.33 (1–2) | 3 (3–3) |
| direction reversals | 0.33 (0–1) | 0.67 (0–1) |

Jev model use per run: 13.0 calls, 27,317 input and 3,642 output tokens, $0.00115 ($0.0069 per hour), mean call latency 125 ms, 0 fallbacks in total.
