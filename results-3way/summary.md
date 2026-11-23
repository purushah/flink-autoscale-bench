# Benchmark summary

Each cell is the mean over repetitions, with the lowest and highest repetition in brackets.

## ramp (3 repetitions)

| Metric | Built-in | Jev | Claude |
|---|---|---|---|
| p50 latency (ms) | 5,330 (156–15,675) | 143 (130–164) | 188 (126–306) |
| p95 latency (ms) | 24,963 (11,042–45,286) | 25,944 (13,982–33,936) | 27,304 (21,217–30,692) |
| p99 latency (ms) | 41,287 (20,584–68,085) | 34,608 (25,154–41,517) | 34,456 (29,937–37,622) |
| seconds with p95 > 2000 ms | 118 (41.0–264) | 64.7 (44.0–90.0) | 74.7 (45.0–130) |
| recovery time (s) | 225 (155–360) | 292 (238–360) | 255 (199–360) |
| throughput (rec/s) | 1,310 (1,283–1,325) | 1,348 (1,258–1,418) | 1,336 (1,282–1,372) |
| largest backlog (records) | 43,288 (25,007–72,735) | 43,100 (27,626–54,934) | 52,603 (49,051–54,756) |
| average backlog (records) | 15,062 (7,297–28,227) | 9,610 (8,919–9,989) | 9,132 (6,715–11,601) |
| average total parallelism | 6.39 (6.12–6.93) | 10.2 (9.75–11.0) | 9.96 (9.5–10.3) |
| peak Aggregate parallelism | 6 (5–8) | 7.67 (7–8) | 8 (8–8) |
| average TaskManager pods | 1.64 (1.57–1.67) | 1.64 (1.57–1.71) | 1.56 (1.54–1.57) |
| scaling actions | 2.33 (2–3) | 3 (3–3) | 2.33 (2–3) |
| direction reversals | 0 (0–0) | 0 (0–0) | 0 (0–0) |

Jev model use per run: 11.7 calls, 25,053 input and 3,296 output tokens, $0.00105 ($0.0070 per hour), mean call latency 133 ms, 0 fallbacks in total.

Claude model use per run: 14.0 calls, 25,489 input and 7,401 output tokens, $0.00625 ($0.0417 per hour), mean call latency 3,386 ms, 0 fallbacks in total.

## spike (3 repetitions)

| Metric | Built-in | Jev | Claude |
|---|---|---|---|
| p50 latency (ms) | 30,600 (23,117–35,673) | 12,455 (5,848–25,253) | 26,104 (9,109–35,918) |
| p95 latency (ms) | 92,256 (90,153–93,633) | 59,119 (38,851–86,923) | 77,387 (56,019–109,016) |
| p99 latency (ms) | 104,073 (96,316–117,237) | 68,739 (47,144–99,199) | 85,148 (63,959–120,778) |
| seconds with p95 > 2000 ms | 186 (160–204) | 142 (129–156) | 159 (115–197) |
| recovery time (s) | 350 (329–360) | 273 (244–328) | 239 (150–323) |
| throughput (rec/s) | 896 (886–901) | 891 (878–907) | 913 (904–927) |
| largest backlog (records) | 137,964 (131,043–146,030) | 100,905 (88,190–126,144) | 106,666 (100,319–116,429) |
| average backlog (records) | 30,959 (25,097–35,046) | 17,540 (12,833–26,473) | 27,197 (15,978–36,500) |
| average total parallelism | 7.23 (6.07–9.51) | 9.73 (8.89–10.5) | 10.8 (10.4–11.5) |
| peak Aggregate parallelism | 6 (5–8) | 7.33 (6–8) | 8 (8–8) |
| average TaskManager pods | 1.69 (1.68–1.7) | 1.65 (1.55–1.75) | 1.71 (1.55–1.8) |
| scaling actions | 2.67 (2–3) | 3.67 (3–4) | 2 (2–2) |
| direction reversals | 0.67 (0–1) | 1.33 (1–2) | 0 (0–0) |

Jev model use per run: 10.3 calls, 21,661 input and 2,791 output tokens, $0.00091 ($0.0055 per hour), mean call latency 133 ms, 0 fallbacks in total.

Claude model use per run: 16.3 calls, 29,944 input and 9,805 output tokens, $0.00790 ($0.0474 per hour), mean call latency 3,907 ms, 0 fallbacks in total.

## scale_down (3 repetitions)

| Metric | Built-in | Jev | Claude |
|---|---|---|---|
| p50 latency (ms) | 12,017 (135–35,780) | 51,669 (15,952–91,691) | 133 (128–140) |
| p95 latency (ms) | 45,489 (204–114,899) | 131,599 (55,277–219,492) | 6,025 (221–17,172) |
| p99 latency (ms) | 53,025 (276–131,695) | 142,644 (60,516–234,775) | 13,833 (8,606–21,209) |
| seconds with p95 > 2000 ms | 40.3 (2–94.0) | 116 (71.0–178) | 12.0 (5–23.0) |
| recovery time (s) | 173 (93.0–300) | 227 (130–300) | 242 (206–300) |
| throughput (rec/s) | 704 (555–910) | 963 (825–1,080) | 585 (559–631) |
| largest backlog (records) | 71,434 (13,733–157,740) | 157,431 (89,527–225,892) | 23,216 (13,148–40,330) |
| average backlog (records) | 10,575 (3,173–24,208) | 39,597 (12,058–75,466) | 3,369 (2,677–4,119) |
| average total parallelism | 12.3 (11.9–12.5) | 11.5 (10.7–12.0) | 9.51 (9.06–9.79) |
| peak Aggregate parallelism | 8 (8–8) | 8 (8–8) | 8 (8–8) |
| average TaskManager pods | 2 (2–2) | 1.8 (1.75–1.9) | 1.75 (1.62–2) |
| scaling actions | 1 (1–1) | 2.67 (2–3) | 2 (2–2) |
| direction reversals | 0 (0–0) | 0.67 (0–1) | 0 (0–0) |

Jev model use per run: 11.0 calls, 23,416 input and 2,814 output tokens, $0.00098 ($0.0056 per hour), mean call latency 158 ms, 0 fallbacks in total.

Claude model use per run: 12.7 calls, 24,767 input and 6,839 output tokens, $0.00590 ($0.0337 per hour), mean call latency 3,657 ms, 0 fallbacks in total.

## skew (3 repetitions)

| Metric | Built-in | Jev | Claude |
|---|---|---|---|
| p50 latency (ms) | 138 (133–147) | 948 (128–2,586) | 140 (128–161) |
| p95 latency (ms) | 4,034 (272–9,639) | 16,429 (249–34,237) | 7,603 (4,117–12,030) |
| p99 latency (ms) | 7,603 (4,814–12,448) | 22,084 (7,372–40,863) | 13,021 (9,717–16,985) |
| seconds with p95 > 2000 ms | 33.0 (6–74.0) | 81.3 (6–197) | 59.3 (22.0–127) |
| recovery time (s) | 151 (95.0–260) | 170 (0–300) | 276 (227–300) |
| throughput (rec/s) | 907 (906–907) | 918 (907–936) | 925 (914–932) |
| largest backlog (records) | 12,542 (10,056–17,283) | 24,142 (7,048–45,051) | 16,806 (13,604–22,722) |
| average backlog (records) | 5,002 (3,913–5,607) | 7,911 (4,390–13,886) | 5,384 (3,137–6,641) |
| average total parallelism | 10.1 (9.58–10.6) | 8.92 (7.69–10.2) | 9.78 (9.18–10.4) |
| peak Aggregate parallelism | 8 (8–8) | 7 (5–8) | 6.67 (6–8) |
| average TaskManager pods | 2 (2–2) | 1.77 (1.45–1.94) | 2 (2–2) |
| scaling actions | 1 (1–1) | 2.33 (1–3) | 1.67 (1–2) |
| direction reversals | 0 (0–0) | 0 (0–0) | 0.33 (0–1) |

Jev model use per run: 11.7 calls, 24,465 input and 3,271 output tokens, $0.00103 ($0.0069 per hour), mean call latency 134 ms, 0 fallbacks in total.

Claude model use per run: 13.0 calls, 23,640 input and 7,744 output tokens, $0.00624 ($0.0416 per hour), mean call latency 3,674 ms, 0 fallbacks in total.

## capacity (3 repetitions)

| Metric | Built-in | Jev | Claude |
|---|---|---|---|
| p50 latency (ms) | 141 (137–148) | 491 (135–1,201) | 177 (132–259) |
| p95 latency (ms) | 2,034 (263–3,447) | 19,248 (2,779–30,218) | 18,717 (211–51,479) |
| p99 latency (ms) | 8,541 (6,546–12,292) | 28,726 (9,474–43,324) | 24,442 (2,412–64,390) |
| seconds with p95 > 2000 ms | 22.3 (4–47.0) | 96.0 (16.0–201) | 83.3 (3–190) |
| recovery time (s) | 153 (0–271) | 234 (184–309) | 188 (150–249) |
| throughput (rec/s) | 810 (806–817) | 831 (814–841) | 822 (804–854) |
| largest backlog (records) | 8,417 (4,970–10,269) | 22,815 (12,206–29,910) | 23,831 (7,371–52,224) |
| average backlog (records) | 3,953 (2,679–4,638) | 6,348 (4,524–8,256) | 7,249 (4,682–12,134) |
| average total parallelism | 8.81 (6–10.3) | 9.48 (8.18–10.9) | 9.81 (9.51–10.2) |
| peak Aggregate parallelism | 6.67 (4–8) | 6.67 (5–8) | 7.33 (6–8) |
| average TaskManager pods | 1.67 (1–2) | 1.72 (1.61–1.87) | 1.98 (1.95–2) |
| scaling actions | 1.33 (1–2) | 3.33 (3–4) | 1.67 (1–3) |
| direction reversals | 0 (0–0) | 0.67 (0–1) | 0.33 (0–1) |

Jev model use per run: 9.67 calls, 19,962 input and 2,685 output tokens, $0.00084 ($0.0050 per hour), mean call latency 180 ms, 0 fallbacks in total.

Claude model use per run: 14.7 calls, 27,117 input and 8,324 output tokens, $0.00687 ($0.0412 per hour), mean call latency 3,642 ms, 0 fallbacks in total.
