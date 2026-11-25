"""Model of the bookkeeping behind Flink's `pendingRecords` for one Kafka source reader.

Supports the INFERRED part of signal_mechanism.md (section "Real mechanism"). It is a model, not
a measurement: it replays the rules below for ONE reader that owns P partitions on one broker and
emits records only as fast as the downstream can absorb them (a backpressured source), and prints
the true backlog next to the sum of the per-partition gauges, which is what Flink's KafkaSource
reports as `pendingRecords`.

Consumer rules, each taken from apache/kafka tag 3.4.0 (the client bundled with
flink-connector-kafka 3.2.0 to 3.4.0), clients/.../consumer/internals/Fetcher.java:
  * a partition is fetchable only while it has no buffered CompletedFetch     fetchablePartitions(), L1135-1144
  * a fetch response carries at most F records per partition, plus the HW      max.partition.fetch.bytes
  * the HW is stored when the CompletedFetch is initialised, i.e. when it      initializeCompletedFetch(), L1310-1313
    reaches the head of the completedFetches queue
  * a poll returns at most max.poll.records records, one partition at a time   collectFetch(), L646-698
  * after each such step: gauge[tp] = storedHW[tp] - position[tp]              fetchRecords(), L735-737
  * the next fetches are sent after a poll that returned records              KafkaConsumer.poll(), L1243-1253

Reader rule (Flink's source reader base): the split fetcher thread polls the consumer and puts each
batch into a hand-over queue of `source.reader.element.queue.capacity` batches (default 2); it
blocks on a full queue with one more batch in hand, and the task thread emits a further one. With
`handover` = 3 the consumer's position is therefore up to four batches ahead of the last record
the reader emitted. Those records are in neither the gauge nor, any longer, in the topic's lag as
the consumer sees it. `handover` = 0 gives the consumer alone.

The true backlog is counted to the last record EMITTED, as the benchmark measures it (broker end
offsets minus the reader's `currentOffset` gauges).

Simplifications: no network latency, a fetch is answered before the next poll, uniform arrivals
over partitions, no job restart, no fetch.max.wait.ms long-poll.

Run:  python3 fetch_lag_sim.py
"""
import collections


def run(P, F, max_poll, lam, cap, t_end, sample_every=5.0, handover=3, dt=0.02, node_gate=False):
    """Returns rows (t, true_backlog, sum_of_gauges, capacity, arrival_rate).

    node_gate=True applies the rule of Kafka clients from 4.0.0 on (KAFKA-17182,
    AbstractFetch.prepareFetchRequests, `bufferedNodes`): no partition of a broker is fetched while
    the consumer still holds data of any partition of that broker. With one broker, as here, all
    partitions are then fetched together once the last buffered response is used up. The default
    is the rule of 3.4.0 and 3.9.1 (a buffered partition alone is left out), which is the client
    the benchmark ran with."""
    t = 0.0
    log_end = [0.0] * P
    pos = [0.0] * P
    hw = [None] * P
    gauge = [0.0] * P  # Kafka `Value` stat: last recorded value
    queue = []  # completedFetches: (partition, records, hw_in_response)
    nil = None  # nextInLineFetch: [partition, records_left]
    batches = collections.deque()  # polled, not yet emitted; batches[0] is being emitted
    emitted = 0.0
    next_sample = 0.0
    rows = []

    def send_fetches():
        buffered = {q[0] for q in queue}
        if nil is not None and nil[1] > 0:
            buffered.add(nil[0])
        if node_gate and buffered:
            return
        for i in range(P):
            if i in buffered:
                continue
            available = int(log_end[i] - pos[i])
            if available > 0:
                queue.append((i, min(available, F), int(log_end[i])))

    def poll():
        nonlocal nil
        remaining = max_poll
        returned = 0
        while remaining > 0:
            if nil is None or nil[1] <= 0:
                if not queue:
                    break
                tp, n, h = queue.pop(0)
                hw[tp] = h  # high watermark stored at initialisation
                nil = [tp, n]
            tp = nil[0]
            k = min(remaining, nil[1])
            nil[1] -= k
            pos[tp] += k
            remaining -= k
            returned += k
            gauge[tp] = hw[tp] - pos[tp]  # recorded after every fetchRecords() call
        send_fetches()
        return returned

    def refill():
        # The split fetcher polls as long as it can hand a batch over.
        while len(batches) < handover + 1:
            k = poll()
            if k == 0:
                break
            batches.append(k)

    while t < t_end:
        while t >= next_sample:
            rows.append((next_sample, sum(log_end) - emitted, sum(gauge), cap(t), lam(t)))
            next_sample += sample_every
        refill()
        budget = cap(t) * dt
        while budget > 1e-9 and batches:
            k = min(budget, batches[0])
            batches[0] -= k
            emitted += k
            budget -= k
            if batches[0] <= 1e-9:
                batches.popleft()
                refill()
        rate = lam(t)
        for i in range(P):
            log_end[i] += rate / P * dt
        t += dt
    return rows


def report(rows, title, t_from, t_to, floor=20000):
    print("\n" + title)
    print("   t[s]  arrival  capacity   true_lag  pendingRecords  ratio")
    for t, lag, g, c, a in rows:
        if t_from <= t <= t_to:
            ratio = g / lag if lag > 0 else float("nan")
            print("  %5.0f  %7.0f  %8.0f  %9.0f  %14.0f  %5.2f" % (t, a, c, lag, g, ratio))
    over = [r for r in rows if r[1] > floor]
    if over:
        print(
            "  while true lag > %d: max pendingRecords %.0f, mean %.0f, mean ratio %.3f, min ratio %.3f"
            % (
                floor,
                max(r[2] for r in over),
                sum(r[2] for r in over) / len(over),
                sum(r[2] / r[1] for r in over) / len(over),
                min(r[2] / r[1] for r in over),
            )
        )


if __name__ == "__main__":
    RECORDS_PER_MIB = 21000  # 1 MiB / ~50 bytes per record on the wire (40-byte value)

    # Scenario A: load step 400 -> 2000 rec/s at t=60 for 120 s, capacity 1000 then 2500 rec/s.
    spike = lambda t: 2000.0 if 60 <= t < 180 else 400.0
    capacity = lambda t: 1000.0 if t < 135 else 2500.0
    for P in (8, 4, 1):
        rows = run(P, RECORDS_PER_MIB, 500, spike, capacity, 260)
        report(rows, "A. spike, %d partition(s) on one reader, max.partition.fetch.bytes = 1 MiB" % P, 55, 210)
    rows = run(8, 500, 500, spike, capacity, 260)
    report(rows, "A. spike, 8 partitions on one reader, fetch capped at 500 records per partition", 55, 210)

    # Scenario B: a new consumer starts with ~110,000 records of backlog (job restart).
    restart = lambda t: 220000.0 if t < 0.5 else (2000.0 if t < 45 else 400.0)
    rows = run(8, RECORDS_PER_MIB, 500, restart, lambda t: 2500.0, 110)
    report(rows, "B. restart with ~110k backlog, 8 partitions, max.partition.fetch.bytes = 1 MiB", 5, 90)
    rows = run(8, 500, 500, restart, lambda t: 2500.0, 110)
    report(rows, "B. restart with ~110k backlog, 8 partitions, fetch capped at 500 records per partition", 5, 90)
