from kafka import KafkaConsumer, TopicPartition
import argparse, time, sys

def slow_consume(bootstrap, topic, group, sleep_ms, commit_every, max_poll_records):
    c = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,            # we commit manually
        auto_offset_reset="earliest",        # start from beginning on first run
        max_poll_records=max_poll_records,
        request_timeout_ms=15000,
        consumer_timeout_ms=0,               # block forever until Ctrl+C
    )
    seen = 0
    print(f"Consuming from topic='{topic}', group='{group}' "
          f"(sleep={sleep_ms}ms/message, commit_every={commit_every})")

    try:
        while True:
            batch = c.poll(timeout_ms=1000, max_records=max_poll_records)
            total = sum(len(v) for v in batch.values())
            if total == 0:
                continue
            for tp, recs in batch.items():
                for r in recs:
                    # simulate slow processing
                    time.sleep(sleep_ms / 1000.0)
                    seen += 1
                    if seen % commit_every == 0:
                        c.commit()  # sync commit
                        print(f"Committed after {seen} messages")
    except KeyboardInterrupt:
        print("\nStopping… committing final offsets")
        try:
            c.commit()
        except Exception:
            pass
        c.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="x-topic")
    ap.add_argument("--group", default="x-lag-demo")
    ap.add_argument("--sleep-ms", type=int, default=50)          # increase to make lag bigger
    ap.add_argument("--commit-every", type=int, default=500)     # commit less often -> more lag
    ap.add_argument("--max-poll-records", type=int, default=100)
    args = ap.parse_args()
    slow_consume(args.bootstrap, args.topic, args.group,
                 args.sleep_ms, args.commit_every, args.max_poll_records)



# python .\consumer.py --topic my-topic --group lag-demo --sleep-ms 50 --commit-every 500
# Consuming from topic='my-topic', group='lag-demo' (sleep=50ms/message, commit_every=500)
# Committed after 500 messages
# Committed after 1000 messages
# Committed after 1500 messages
# Committed after 2000 messages
# Committed after 2500 messages
# Committed after 3000 messages
# Committed after 3500 messages
# Committed after 4000 messages
# Committed after 4500 messages
# Committed after 5000 messages