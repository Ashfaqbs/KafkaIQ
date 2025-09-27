# smoke_test.py
import json
import re
import subprocess
import time
from typing import Dict, List, Optional

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import (
    ConfigResource,
    ConfigResourceType,
    KafkaAdminClient,
    NewTopic,
)
from kafka.errors import KafkaError, TopicAlreadyExistsError


class KafkaMonitor:
    def __init__(self, bootstrap_servers: str):
        self.admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="kafka-monitor",
        )

    # ---------- Cluster ----------
    def check_cluster(self) -> Dict:
        meta = self.admin.describe_cluster()
        brokers = meta.get("brokers", [])
        controller = meta.get("controller_id")
        cluster_id = meta.get("cluster_id")
        print(f"Cluster ID: {cluster_id}")
        print(f"Broker count: {len(brokers)}")
        print(f"Controller Broker ID: {controller}")
        if len(brokers) == 0:
            print("ERROR: No brokers found in cluster (cluster may be down!)")
        return meta

    # ---------- Brokers ----------
    def check_brokers(self) -> List[Dict]:
        meta = self.admin.describe_cluster()
        brokers = meta.get("brokers", [])
        controller_id = meta.get("controller_id")
        if not brokers:
            print("No broker information available.")
            return []
        for b in brokers:
            role = "Controller" if b.get("node_id") == controller_id else "Broker"
            print(f"Broker {b.get('node_id')} at {b.get('host')}:{b.get('port')} ({role})")
        return brokers

    # ---------- Topics (health) ----------
    def check_topics(self) -> List[str]:
        issues: List[str] = []
        topics_meta = self.admin.describe_topics()
        for t in topics_meta:
            name = t["topic"]
            parts = t.get("partitions", [])
            rf = len(parts[0]["replicas"]) if parts else 0
            print(f"Topic '{name}': {len(parts)} partitions, replication-factor {rf}")
            for p in parts:
                pid = p["partition"]
                leader = p.get("leader", -1)
                replicas = p.get("replicas", [])
                isr = p.get("isr", [])
                if len(isr) < len(replicas):
                    issues.append(f"{name}[{pid}] under-replicated: ISR {len(isr)}/{len(replicas)}")
                if leader == -1:
                    issues.append(f"{name}[{pid}] has NO LEADER (offline partition)")
        if issues:
            print("Detected topic issues:")
            for i in issues:
                print(" - " + i)
        else:
            print("No obvious replication issues found in topics.")
        return issues

    # ---------- Consumer groups ----------
    def list_consumer_groups(self) -> List[str]:
        try:
            groups = self.admin.list_consumer_groups()  # [(group_id, protocol_type)]
            ids = [g[0] for g in groups]
            print(f"Found {len(ids)} consumer groups")
            for gid in ids:
                print(f" - {gid}")
            return ids
        except KafkaError as e:
            print(f"ISSUE: list_consumer_groups failed: {e}")
            return []

    def get_group_offsets(self, group_id: str) -> Dict[str, Dict[int, int]]:
        result: Dict[str, Dict[int, int]] = {}
        try:
            offsets = self.admin.list_consumer_group_offsets(group_id)
            for tp, meta in offsets.items():
                result.setdefault(tp.topic, {})[tp.partition] = meta.offset
            print(json.dumps(result, indent=2))
            return result
        except KafkaError as e:
            print(f"ISSUE: list_consumer_group_offsets failed for '{group_id}': {e}")
            return {}

    def check_consumer_lag(
        self,
        group_id: str,
        topics: Optional[List[str]] = None,
        print_output: bool = True,
    ) -> Dict[str, Dict[int, int]]:
        """
        lag = end_offset (broker latest) - committed_offset (group)
        """
        lags: Dict[str, Dict[int, int]] = {}
        try:
            if topics is None:
                topics = [t["topic"] for t in self.admin.describe_topics() if not t.get("is_internal", False)]
            c = KafkaConsumer(
                bootstrap_servers=self.admin.config["bootstrap_servers"],
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=45000,  # must be > session_timeout_ms
                consumer_timeout_ms=5000,
            )
            for t in topics:
                parts = c.partitions_for_topic(t)
                if not parts:
                    continue
                tps = [TopicPartition(t, p) for p in parts]
                end_offsets = c.end_offsets(tps)
                lags[t] = {}
                for tp in tps:
                    committed = c.committed(tp)
                    end = end_offsets.get(tp, 0)
                    lags[t][tp.partition] = end if committed is None else max(end - committed, 0)
            if print_output:
                print("Consumer lag:")
                print(json.dumps(lags, indent=2))
            c.close()
            return lags
        except KafkaError as e:
            print(f"ISSUE: Lag computation failed for '{group_id}': {e}")
            return {}

    # ---------- Topic config (robust across protocol variations) ---------- READY
    def check_topic_config(self, topic: str) -> Dict[str, str]:
        try:
            responses = self.admin.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic)],
                include_synonyms=False,
            )
        except KafkaError as e:
            print(f"ISSUE: describe_configs failed for '{topic}': {e}")
            return {}

        cfg: Dict[str, str] = {}
        resp_list = responses if isinstance(responses, list) else [responses]
        for resp in resp_list:
            resources = getattr(resp, "resources", []) or []
            for res in resources:
                err = 0
                rname = topic
                configs = None
                if isinstance(res, (list, tuple)):
                    # (err_code, resource_type, resource_name, configs) OR
                    # (err_code, err_msg, resource_type, resource_name, configs)
                    if len(res) >= 5:
                        err, _err_msg, _rtype, rname, configs = res[:5]
                    elif len(res) >= 4:
                        err, _rtype, rname, configs = res[:4]
                    else:
                        configs = res[-1] if res else None
                else:
                    err = getattr(res, "error_code", 0)
                    rname = getattr(res, "resource_name", topic)
                    configs = getattr(res, "configs", None)

                if err and err != 0:
                    print(f"ERROR: describe_configs({rname}) returned error_code={err}")
                    continue
                if not configs:
                    continue

                # config entry: at least (name, value), ignore extra flags
                for entry in configs:
                    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                        name, value = entry[0], entry[1]
                    else:
                        name = getattr(entry, "name", None)
                        value = getattr(entry, "value", None)
                    if name is not None:
                        cfg[str(name)] = None if value is None else str(value)

        if cfg:
            print(f"\n=== Topic Config ({topic}) ===")
            print(json.dumps(cfg, indent=2))
            # simple heuristics
            try:
                r_ms = int(cfg.get("retention.ms", "-1"))
                if r_ms > 0 and r_ms < 5 * 60 * 1000:
                    print(f"RISK: retention.ms={r_ms} is very small; slow consumers may lose data.")
            except ValueError:
                pass
            cp = cfg.get("cleanup.policy")
            if cp and cp not in ("delete", "compact", "compact,delete", "delete,compact"):
                print(f"NOTE: unusual cleanup.policy: {cp}")
        else:
            print(f"No configs returned for topic '{topic}'.")
        return cfg

    # ---------- Topic admin & listing ----------
    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[Dict[str, str]] = None,
        if_not_exists: bool = True,
        timeout_ms: int = 15000,
    ) -> bool:
        try:
            new_topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=configs or {},
            )
            self.admin.create_topics([new_topic], timeout_ms=timeout_ms, validate_only=False)
            print(f"Created topic '{name}' (P={num_partitions}, RF={replication_factor})")
            return True
        except TopicAlreadyExistsError:
            if if_not_exists:
                print(f"Topic '{name}' already exists (skipped).")
                return False
            raise
        except KafkaError as e:
            print(f"ISSUE: create_topic failed for '{name}': {e}")
            return False

    def list_topic_names(self, include_internal: bool = False) -> List[str]:
        try:
            topics_meta = self.admin.describe_topics()
            names: List[str] = []
            for t in topics_meta:
                if not include_internal and t.get("is_internal", False):
                    continue
                names.append(t["topic"])
            print(json.dumps(names, indent=2))
            return names
        except KafkaError as e:
            print(f"ISSUE: list_topic_names failed: {e}")
            return []

    def list_topics_detailed(self, include_internal: bool = False) -> List[Dict]:
        try:
            topics_meta = self.admin.describe_topics()
            out: List[Dict] = []
            for t in topics_meta:
                if not include_internal and t.get("is_internal", False):
                    continue
                parts = t.get("partitions", [])
                rf = len(parts[0]["replicas"]) if parts else 0
                out.append(
                    {
                        "topic": t["topic"],
                        "partitions": len(parts),
                        "replication_factor": rf,
                        "is_internal": bool(t.get("is_internal", False)),
                    }
                )
            print(json.dumps(out, indent=2))
            return out
        except KafkaError as e:
            print(f"ISSUE: list_topics_detailed failed: {e}")
            return []

    # ---------- Broker resource usage via Docker ----------
    def broker_resource_usage(
        self,
        container: str = "kafka",
        logdir: str = "/kafka/kafka-logs",
    ) -> Dict[str, Dict]:
        result: Dict[str, Dict] = {"docker_stats": {}, "disk": {}}

        # docker stats
        try:
            proc = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                line = proc.stdout.strip().splitlines()[-1]
                result["docker_stats"] = json.loads(line)
            else:
                msg = proc.stderr.strip() or "docker stats returned no data"
                print(f"NOTE: docker stats issue: {msg}")
        except Exception as e:
            print(f"NOTE: cannot run docker stats: {e}")

        # df (filesystem usage)
        try:
            proc = subprocess.run(
                ["docker", "exec", container, "sh", "-lc", f"df -P {logdir} | tail -1"],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                cols = proc.stdout.split()
                if len(cols) >= 6:
                    result["disk"]["df"] = {
                        "filesystem": cols[0],
                        "size_kb": int(cols[1]),
                        "used_kb": int(cols[2]),
                        "avail_kb": int(cols[3]),
                        "use_pct": cols[4],
                        "mount": cols[5],
                    }
        except Exception as e:
            print(f"NOTE: cannot run df in container: {e}")

        # du (total bytes under logdir)
        def _du_bytes(path: str) -> Optional[int]:
            for du_cmd in [f"du -sb {path}", f"du -sk {path}"]:
                p = subprocess.run(
                    ["docker", "exec", container, "sh", "-lc", du_cmd],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if p.returncode == 0 and p.stdout.strip():
                    try:
                        size_str = p.stdout.strip().split()[0]
                        size = int(size_str)
                        if " -sk " in du_cmd:
                            size *= 1024
                        return size
                    except Exception:
                        continue
            return None

        try:
            total_bytes = _du_bytes(logdir)
            if total_bytes is not None:
                result["disk"]["logdir_total_bytes"] = total_bytes
        except Exception as e:
            print(f"NOTE: cannot run du in container: {e}")

        print(json.dumps(result, indent=2))
        return result

    # ---------- Topic disk usage (Kafka API or Docker fallback) ----------
    def topic_disk_usage(
        self,
        topic: str,
        container: str = "kafka",
        logdir: str = "/kafka/kafka-logs",
    ) -> Dict[int, int]:
        # Prefer Kafka Admin API (if exposed in your kafka-python build)
        try:
            if hasattr(self.admin, "describe_log_dirs"):
                resp = self.admin.describe_log_dirs()
                sizes: Dict[int, int] = {}
                for (err, log_dir, topics) in getattr(resp, "log_dirs", []):
                    if err != 0:
                        continue
                    for (tname, partitions) in topics:
                        if tname != topic:
                            continue
                        for (p_idx, p_size, *_rest) in partitions:
                            sizes[p_idx] = sizes.get(p_idx, 0) + int(p_size)
                if sizes:
                    print(
                        json.dumps(
                            {"topic": topic, "total_bytes": sum(sizes.values()), "by_partition": sizes},
                            indent=2,
                        )
                    )
                    return sizes
        except Exception:
            pass  # fall back to docker

        # Docker fallback: sum <logdir>/<topic>-*
        try:
            ls_cmd = f"ls -d {logdir}/{topic}-* 2>/dev/null"
            proc = subprocess.run(
                ["docker", "exec", container, "sh", "-lc", ls_cmd],
                capture_output=True,
                text=True,
                check=False,
            )
            dirs = [d for d in proc.stdout.splitlines() if d.strip()]
            if not dirs:
                print(f"No log directories found for topic '{topic}' under {logdir}")
                return {}

            def _du_one(path: str) -> Optional[int]:
                for du_cmd in [f"du -sb '{path}'", f"du -sk '{path}'"]:
                    p = subprocess.run(
                        ["docker", "exec", container, "sh", "-lc", du_cmd],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if p.returncode == 0 and p.stdout.strip():
                        try:
                            size_str = p.stdout.strip().split()[0]
                            size = int(size_str)
                            if " -sk " in du_cmd:
                                size *= 1024
                            return size
                        except Exception:
                            continue
                return None

            sizes: Dict[int, int] = {}
            for d in dirs:
                m = re.search(r"-([0-9]+)$", d)
                p_idx = int(m.group(1)) if m else -1
                sz = _du_one(d) or 0
                sizes[p_idx] = sizes.get(p_idx, 0) + sz

            print(
                json.dumps({"topic": topic, "total_bytes": sum(sizes.values()), "by_partition": sizes}, indent=2)
            )
            return sizes
        except Exception as e:
            print(f"ISSUE: topic_disk_usage fallback failed for '{topic}': {e}")
            return {}


if __name__ == "__main__":
    monitor = KafkaMonitor(bootstrap_servers="localhost:9092")

    print("=== Cluster Check ===")
    monitor.check_cluster()

    print("\n=== Broker Check ===")
    monitor.check_brokers()

    print("\n=== Topic Check ===")
    monitor.check_topics()

    print("\n=== Consumer Groups ===")
    groups = monitor.list_consumer_groups()

    gid = "lag-demo"
    print("\n=== Group Offsets (lag-demo) ===")
    offsets = monitor.get_group_offsets(gid)

    print("\n=== Lag (lag-demo) ===")
    monitor.check_consumer_lag(group_id=gid, topics=list(offsets.keys()))

    # Optional: create a demo topic idempotently
    # print("\n=== Create Topic (demo-quick) ===")
    # monitor.create_topic("demo-quick", num_partitions=1, replication_factor=1, if_not_exists=True)

    print("\n=== List Topic Names ===")
    monitor.list_topic_names()

    print("\n=== List Topics Detailed ===")
    monitor.list_topics_detailed()

    print("\n=== Topic Config (my-topic) ===")
    monitor.check_topic_config("my-topic")

    print("\n=== Broker Resource Usage (docker stats + disk) ===")
    monitor.broker_resource_usage(container="kafka", logdir="/kafka/kafka-logs")

    print("\n=== Topic Disk Usage (my-topic) ===")
    monitor.topic_disk_usage("my-topic", container="kafka", logdir="/kafka/kafka-logs")
