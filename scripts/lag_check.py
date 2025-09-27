# lag_check.py
from kafka import KafkaConsumer, TopicPartition
import argparse, json

def compute_lag(bootstrap, group, topics):
    c = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,
        # must be strictly greater than session_timeout_ms (default 10000)
        request_timeout_ms=45000,
        consumer_timeout_ms=5000,
        # optional: make first run deterministic if the group has no commits
        auto_offset_reset="earliest",
        # session_timeout_ms=10000,  # (implicit default) keep as-is or tune if you want
    )
    result = {}
    for t in topics:
        parts = c.partitions_for_topic(t)
        if not parts:
            print(f"Skip: no partitions for topic '{t}'")
            continue
        tps = [TopicPartition(t, p) for p in parts]
        end_offsets = c.end_offsets(tps)
        result[t] = {}
        for tp in tps:
            committed = c.committed(tp)  # None if the group never committed
            end = end_offsets.get(tp, 0)
            lag = end if committed is None else max(end - committed, 0)
            result[t][tp.partition] = lag
    print(json.dumps(result, indent=2))
    c.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="lag-demo")
    ap.add_argument("--topics", nargs="+", default=["my-topic"])
    args = ap.parse_args()
    compute_lag(args.bootstrap, args.group, args.topics)


## python .\lag_check.py --group lag-demo --topics my-topic