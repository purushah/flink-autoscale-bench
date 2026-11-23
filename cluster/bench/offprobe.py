"""Probe for ISSUES.md #141: how far from the true arrival rate is a rate computed from two readings
of a topic's end offsets, as a function of the time between the readings and of how the readings are
stamped?

The step-aware demand (BrokerLag.java) reads the end offsets twice within one evaluation, about half a
second apart, and takes the difference as the arrival rate "now". In Tests C and A that reading was
between 0.18 and 2.6 times the load. This probe measures such readings against a load whose rate is
known, without Flink and without the operator.

It creates one topic, produces to it with the harness's producer settings and pacing at the rates given
(by default three jumps from 400 to 4,800 records/s), and every CYCLE seconds reads the end offsets at
about 0, 0.45, 1.0 and 2.0 s. A reading is one listing per partition leader, one after the other. The
rate between two readings is computed in three ways:
   after    one time stamp per reading, taken when the last answer is in (the plugin in Tests C and A)
   middle   one time stamp per reading, the middle between the first request and the last answer
            (the plugin since 23 Nov)
   leader   every leader's partitions with the middle of that leader's own listing
The topic is deleted at the end.

   python3 offprobe.py <out.csv> [--conf cluster.json] [--levels 400:45,4800:60,...] [--partitions 16]

With --conf it logs in as bench_cluster.py does (the cluster); without, it uses the laptop's broker.
The true rate of a level is the rate between the first and the last reading that lie 15 s or more after
the level began. Readings that do not lie inside one level are left out. Output: one line per reading
in the CSV, and per level, apart for the first 15 s after a change, the ratio reading / true rate per
span and method: lowest, 5%, median, 95%, highest, share within 20%.
"""
import argparse, csv, os, statistics, sys, threading, time

SPANS = (0.45, 1.0, 2.0)
METHODS = ("after", "middle", "leader")
CYCLE = 2.5
SETTLE = 15.0


def clients(conf_path):
    """(brokers, admin class, consumer class, producer factory, replication factor)."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    if conf_path:
        import bench_cluster as bo
        bo.CONF = bo.load_conf(conf_path)
        login = bo.kafka_login()
        brokers = bo.CONF["brokers"]
        from confluent_kafka import Producer

        def producer():
            p = Producer({"bootstrap.servers": brokers, "linger.ms": 20, "batch.size": 262144, "acks": 1, **login})
            p.poll(0)
            return lambda topic, value, part: p.produce(topic, value=value, partition=part), lambda: p.poll(0), lambda: p.flush(30)
        return brokers, bo.bench.KafkaAdminClient, bo.bench.KafkaConsumer, producer, int(bo.CONF.get("replication_factor", 1))
    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
    brokers = "127.0.0.1:9094"

    def producer():
        p = KafkaProducer(bootstrap_servers=brokers, linger_ms=20, batch_size=262144, acks=1)
        return lambda topic, value, part: p.send(topic, value=value, partition=part), lambda: None, lambda: p.flush(30)
    return brokers, KafkaAdminClient, KafkaConsumer, producer, 1


class Load(threading.Thread):
    """The harness's send loop (bench_cluster.py LoadGenerator._run): every 50 ms what the rate asks for."""

    def __init__(self, make, topic, partitions, levels):
        super().__init__(daemon=True)
        self.make, self.topic, self.partitions, self.levels = make, topic, partitions, levels
        self.sent, self.error, self.t0, self.done = 0, None, None, False

    def rate(self, t):
        for r, _, until in self.levels:
            if t < until:
                return r
        return None

    def run(self):
        try:
            send, poll, flush = self.make()
            carry, self.t0 = 0.0, time.time()
            last = self.t0
            while True:
                now = time.time()
                r = self.rate(now - self.t0)
                if r is None:
                    break
                carry += r * (now - last)
                last = now
                n = int(carry)
                carry -= n
                ms = int(now * 1000)
                for _ in range(n):
                    send(self.topic, f"{self.sent},k{self.sent % 2000},{ms},0,0".encode(), self.sent % self.partitions)
                    self.sent += 1
                poll()
                time.sleep(max(0.0, 0.05 - (time.time() - now)))
            flush()
        except Exception as e:
            self.error = e
        self.done = True


def quantile(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))]


def rates(a, b):
    """The three rates between reading a and reading b; a reading is (parts, first request, last answer)
    with parts = [(middle, sum of end offsets, seconds the listing took)] per leader."""
    arrived = sum(pb[1] - pa[1] for pa, pb in zip(a[0], b[0]))
    return {
        "after": arrived / (b[2] - a[2]),
        "middle": arrived / ((b[1] + b[2]) / 2 - (a[1] + a[2]) / 2),
        "leader": sum((pb[1] - pa[1]) / (pb[0] - pa[0]) for pa, pb in zip(a[0], b[0])),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--conf", default=None)
    ap.add_argument("--levels", default="400:45,4800:60,400:45,4800:60,400:45,4800:60,400:30", help="rate:seconds,...")
    ap.add_argument("--partitions", type=int, default=16)
    a = ap.parse_args()
    levels, end = [], 0.0
    for part in a.levels.split(","):
        r, _, secs = part.partition(":")
        levels.append((float(r), end, end + float(secs)))
        end += float(secs)
    brokers, Admin, Consumer, make, replicas = clients(a.conf)
    from kafka import TopicPartition
    from kafka.admin import NewTopic
    topic = f"probe-offsets-{int(time.time())}"
    admin = Admin(bootstrap_servers=brokers)
    admin.create_topics([NewTopic(topic, a.partitions, replicas)])
    print(f"topic {topic}: {a.partitions} partitions, replication {replicas}; levels {a.levels}", flush=True)
    rows = []
    try:
        consumer = Consumer(bootstrap_servers=brokers, group_id=None)
        tps = [TopicPartition(topic, p) for p in range(a.partitions)]
        deadline = time.time() + 30
        while time.time() < deadline:  # the leaders are known a moment after the topic exists
            consumer.partitions_for_topic(topic)
            leaders = {tp: consumer._client.cluster.leader_for_partition(tp) for tp in tps}
            if all(v is not None and v >= 0 for v in leaders.values()):
                break
            time.sleep(0.5)
        groups = {}
        for tp, leader in leaders.items():
            groups.setdefault(leader, []).append(tp)
        groups = [groups[k] for k in sorted(groups, key=str)]
        print(f"partitions per leader: {[len(g) for g in groups]}", flush=True)

        def read():
            first = time.time()
            parts = []
            for g in groups:
                sent = time.time()
                ends = consumer.end_offsets(g)
                got = time.time()
                parts.append(((sent + got) / 2, sum(ends.values()), got - sent))
            return parts, first, time.time()

        read()  # connections to every leader before the first timed reading
        load = Load(make, topic, a.partitions, levels)
        load.start()
        while load.t0 is None and not load.done:
            time.sleep(0.01)
        while not load.done:
            start = time.time()
            r0 = read()
            row = {"t": (r0[1] + r0[2]) / 2 - load.t0, "end": sum(p[1] for p in r0[0]),
                   "listing_ms": max(p[2] for p in r0[0]) * 1000}
            for span in SPANS:
                time.sleep(max(0.0, start + span - time.time()))
                r = read()
                row["listing_ms"] = max(row["listing_ms"], max(p[2] for p in r[0]) * 1000)
                row[f"dt_{span}"] = (r[1] + r[2]) / 2 - (r0[1] + r0[2]) / 2
                for m, v in rates(r0, r).items():
                    row[f"{m}_{span}"] = v
            rows.append(row)
            time.sleep(max(0.0, start + CYCLE - time.time()))
        if load.error:
            raise load.error
        print(f"sent {load.sent} records; {len(rows)} readings", flush=True)
    finally:
        try:
            admin.delete_topics([topic])
            print("topic deleted", flush=True)
        except Exception as e:
            print(f"could not delete the topic {topic}: {e}", flush=True)
        admin.close()

    out = []
    for i, (rate, begin, until) in enumerate(levels):
        inside = [r for r in rows if r["t"] >= begin + 0.3 and r["t"] + SPANS[-1] + 0.3 <= until]
        steady = [r for r in inside if r["t"] >= begin + SETTLE]
        if len(steady) < 2:
            continue
        true = (steady[-1]["end"] - steady[0]["end"]) / (steady[-1]["t"] - steady[0]["t"])
        for r in inside:
            out.append({**r, "level": rate, "level_no": i, "true_rate": true,
                        "phase": "steady" if r["t"] >= begin + SETTLE or i == 0 else "after a change"})
    with open(a.out, "w", newline="") as f:
        cols = ["t", "level_no", "level", "phase", "true_rate", "end", "listing_ms"] + [f"dt_{s}" for s in SPANS] + [f"{m}_{s}" for s in SPANS for m in METHODS]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in out:
            w.writerow({k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()})
    for lv in sorted({r["level"] for r in out}, reverse=True):
        for phase in ("after a change", "steady"):
            sel = [r for r in out if r["level"] == lv and r["phase"] == phase]
            if not sel:
                continue
            lat = [r["listing_ms"] for r in sel]
            print(f"level {lv:.0f} records/s, {phase}: {len(sel)} readings, true rate {statistics.mean(r['true_rate'] for r in sel):.0f}; "
                  f"slowest listing of a reading {statistics.median(lat):.0f} ms (median), {max(lat):.0f} ms at most")
            for s in SPANS:
                for m in METHODS:
                    ratios = [r[f"{m}_{s}"] / r["true_rate"] for r in sel]
                    within = sum(0.8 <= x <= 1.2 for x in ratios) / len(ratios)
                    print(f"   span {statistics.mean(r[f'dt_{s}'] for r in sel):.2f} s, {m:6s}: lowest {min(ratios):.2f}, 5% {quantile(ratios, 0.05):.2f}, "
                          f"median {statistics.median(ratios):.2f}, 95% {quantile(ratios, 0.95):.2f}, highest {max(ratios):.2f}; within 20%: {within * 100:.0f}%")


if __name__ == "__main__":
    main()
