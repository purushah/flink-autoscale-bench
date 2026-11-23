# Benchmark summary

Each cell is the mean over repetitions, with the lowest and highest repetition in brackets.

## ramp (1 repetitions)

| Metric | Built-in | Rule | Jev | Claude |
|---|---|---|---|---|
| p50 latency (ms) | 151 (151–151) | 131 (131–131) | 136 (136–136) | 130 (130–130) |
| p95 latency (ms) | 23,108 (23,108–23,108) | 15,123 (15,123–15,123) | 16,573 (16,573–16,573) | 9,460 (9,460–9,460) |
| p99 latency (ms) | 37,939 (37,939–37,939) | 18,211 (18,211–18,211) | 31,281 (31,281–31,281) | 19,865 (19,865–19,865) |
| seconds with p95 > 2000 ms | 98.0 (98.0–98.0) | 58.0 (58.0–58.0) | 145 (145–145) | 49.0 (49.0–49.0) |
| recovery time (s) | 185 (185–185) | 329 (329–329) | 360 (360–360) | 304 (304–304) |
| throughput (rec/s) | 1,350 (1,350–1,350) | 1,375 (1,375–1,375) | 1,335 (1,335–1,335) | 1,371 (1,371–1,371) |
| largest backlog (records) | 33,950 (33,950–33,950) | 35,731 (35,731–35,731) | 29,744 (29,744–29,744) | 19,550 (19,550–19,550) |
| average backlog (records) | 11,263 (11,263–11,263) | 9,736 (9,736–9,736) | 9,571 (9,571–9,571) | 8,057 (8,057–8,057) |
| average total parallelism | 5.96 (5.96–5.96) | 9.38 (9.38–9.38) | 8.44 (8.44–8.44) | 9.21 (9.21–9.21) |
| peak Aggregate parallelism | 5 (5–5) | 8 (8–8) | 8 (8–8) | 8 (8–8) |
| average TaskManager pods | 1.61 (1.61–1.61) | 1.6 (1.6–1.6) | 1.49 (1.49–1.49) | 1.83 (1.83–1.83) |
| scaling actions | 2 (2–2) | 3 (3–3) | 3 (3–3) | 3 (3–3) |
| direction reversals | 0 (0–0) | 1 (1–1) | 1 (1–1) | 0 (0–0) |

Jev model use per run: 11.0 calls, 23,610 input and 3,177 output tokens, $0.00099 ($0.0066 per hour), mean call latency 131 ms, 0 fallbacks in total.

Claude model use per run: 11.0 calls, 19,649 input and 6,058 output tokens, $0.00499 ($0.0330 per hour), mean call latency 3,495 ms, 0 fallbacks in total.

## spike (1 repetitions)

| Metric | Built-in | Rule | Jev | Claude |
|---|---|---|---|---|
| p50 latency (ms) | 28,335 (28,335–28,335) | 13,251 (13,251–13,251) | 25,485 (25,485–25,485) | 6,526 (6,526–6,526) |
| p95 latency (ms) | 81,373 (81,373–81,373) | 22,360 (22,360–22,360) | 75,653 (75,653–75,653) | 45,051 (45,051–45,051) |
| p99 latency (ms) | 95,569 (95,569–95,569) | 30,107 (30,107–30,107) | 86,405 (86,405–86,405) | 52,279 (52,279–52,279) |
| seconds with p95 > 2000 ms | 181 (181–181) | 145 (145–145) | 165 (165–165) | 115 (115–115) |
| recovery time (s) | 360 (360–360) | 278 (278–278) | 272 (272–272) | 161 (161–161) |
| throughput (rec/s) | 915 (915–915) | 882 (882–882) | 911 (911–911) | 866 (866–866) |
| largest backlog (records) | 129,363 (129,363–129,363) | 47,938 (47,938–47,938) | 100,510 (100,510–100,510) | 86,092 (86,092–86,092) |
| average backlog (records) | 28,727 (28,727–28,727) | 12,937 (12,937–12,937) | 25,973 (25,973–25,973) | 13,709 (13,709–13,709) |
| average total parallelism | 5.95 (5.95–5.95) | 8.9 (8.9–8.9) | 8.65 (8.65–8.65) | 9.95 (9.95–9.95) |
| peak Aggregate parallelism | 5 (5–5) | 8 (8–8) | 8 (8–8) | 8 (8–8) |
| average TaskManager pods | 1.67 (1.67–1.67) | 1.55 (1.55–1.55) | 1.56 (1.56–1.56) | 1.77 (1.77–1.77) |
| scaling actions | 3 (3–3) | 3 (3–3) | 3 (3–3) | 2 (2–2) |
| direction reversals | 1 (1–1) | 1 (1–1) | 1 (1–1) | 0 (0–0) |

Jev model use per run: 18.0 calls, 38,863 input and 5,306 output tokens, $0.00163 ($0.0097 per hour), mean call latency 127 ms, 0 fallbacks in total.

Claude model use per run: 17.0 calls, 30,431 input and 8,384 output tokens, $0.00724 ($0.0431 per hour), mean call latency 3,166 ms, 1 fallbacks in total.
