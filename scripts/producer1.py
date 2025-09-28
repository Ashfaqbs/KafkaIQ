from kafka import KafkaProducer
import argparse, time

def produce(bootstrap, topic, n, linger_ms, batch):
    p = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        linger_ms=linger_ms,
        batch_size=batch,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    t0 = time.time()
    for i in range(n):
        p.send(topic, f"msg-{i}")
        # optional tiny sleep to avoid overwhelming small dev setups
        # time.sleep(0.0005)
    p.flush(30)
    dt = time.time() - t0
    print(f"Produced {n} messages to '{topic}' in {dt:.2f}s (~{int(n/max(dt,1)):d}/s)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="x-topic")
    ap.add_argument("--num", type=int, default=50000)
    ap.add_argument("--linger-ms", type=int, default=5)
    ap.add_argument("--batch", type=int, default=32768)
    args = ap.parse_args()
    produce(args.bootstrap, args.topic, args.num, args.linger_ms, args.batch)

# python .\producer.py --topic my-topic --num 50000