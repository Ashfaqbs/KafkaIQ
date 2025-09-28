# app.py
import json
from typing import Any, Dict, List, Optional, Union
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError, TopicAlreadyExistsError
from typing import Union, List


from typing import Union, List
from kafka.admin import KafkaAdminClient

from typing import Union, List
from kafka.admin import KafkaAdminClient

from typing import Union, List
from kafka.admin import KafkaAdminClient

from typing import Union, List
from kafka.admin import KafkaAdminClient

from typing import Union, List
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

class KafkaInspector:
    """
    Holds broker details (object-level) and exposes single-responsibility utilities.
    """

    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        client_id: str = "kafka-inspector",
        request_timeout_ms: int = 45000,          # keep > session_timeout_ms
        api_version_auto_timeout_ms: int = 8000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.request_timeout_ms = request_timeout_ms
        self.api_version_auto_timeout_ms = api_version_auto_timeout_ms
        self._admin = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            request_timeout_ms=self.request_timeout_ms,
            api_version_auto_timeout_ms=self.api_version_auto_timeout_ms,
        )
    
    def list_topics(self, include_internal: bool = False) -> List[str]:
        """
        Return topic names in the cluster.
        By default skips internal topics (e.g., __consumer_offsets).
        """
        try:
            topics_meta = self._admin.describe_topics()  # all topics
            names: List[str] = []
            for t in topics_meta:
                if not include_internal and t.get("is_internal", False):
                    continue
                names.append(t["topic"])
            return sorted(names)
        except KafkaError as e:
            print(f"[LIST_TOPICS] failed: {e}")
            return []



    def alert_summary(self) -> dict:
        """
        Auto-discovers cluster signals + lag and returns OK/WARN/CRITICAL without external params.

        Built-in thresholds (tune here if needed):
          - offline_crit: any offline partition -> CRITICAL
          - urp_warn/urp_crit: under-replicated partitions thresholds
          - lag_warn/lag_crit: per (group, topic) lag thresholds
        """
        import time
        from kafka.errors import KafkaError

        # ---- thresholds (edit here if you want different defaults) ----
        offline_crit = 1
        urp_warn, urp_crit = 1, 5
        lag_warn, lag_crit = 10_000, 100_000
        single_broker_warn = True

        def rank(level: str) -> int:
            return {"OK": 0, "WARN": 1, "CRITICAL": 2}[level]

        overall = "OK"
        signals: list[dict] = []

        # ---- cluster health (uses your existing helper) ----
        cluster = self.get_cluster_details()
        if "error" in cluster:
            return {
                "status": "CRITICAL",
                "signals": [{"level": "CRITICAL", "code": "CLUSTER_UNREACHABLE", "message": cluster["error"], "data": {}}],
                "summary": {"cluster_id": None},
                "timestamp": int(time.time() * 1000),
            }

        cid = cluster.get("cluster_id")
        brokers = cluster.get("broker_count", 0)
        topics_count = cluster.get("topics_count", 0)
        urp = cluster.get("health", {}).get("under_replicated_count", 0)
        offline = cluster.get("health", {}).get("offline_partition_count", 0)
        single_broker = cluster.get("health", {}).get("single_broker", False)

        # offline partitions -> critical
        if offline >= offline_crit:
            signals.append({
                "level": "CRITICAL",
                "code": "OFFLINE_PARTITIONS",
                "message": f"{offline} partition(s) offline (no leader)",
                "data": {"offline_partitions": offline},
            })
            overall = "CRITICAL"

        # URP
        if urp >= urp_crit:
            lvl = "CRITICAL"
        elif urp >= urp_warn:
            lvl = "WARN"
        else:
            lvl = "OK"
        if lvl != "OK":
            signals.append({
                "level": lvl,
                "code": "UNDER_REPLICATED",
                "message": f"{urp} partition(s) under-replicated",
                "data": {"under_replicated": urp},
            })
            if rank(lvl) > rank(overall):
                overall = lvl

        # single broker (optional warning)
        if single_broker_warn and single_broker:
            signals.append({
                "level": "WARN",
                "code": "SINGLE_BROKER",
                "message": "Cluster has a single broker (no replication/failover)",
                "data": {"broker_count": brokers},
            })
            if rank("WARN") > rank(overall):
                overall = "WARN"

        # ---- auto-discover groups & topics they actually consume ----
        lag_summaries: list[dict] = []
        try:
            groups = self._admin.list_consumer_groups()  # [(group_id, protocol_type)]
            group_ids = [g[0] for g in groups]
            for gid in group_ids:
                offsets = self._admin.list_consumer_group_offsets(gid)  # {TopicPartition: OffsetAndMetadata}
                topics_for_group = sorted({tp.topic for tp in offsets.keys()})
                for topic in topics_for_group:
                    lag = self.get_consumer_lag(gid, topic, print_output=False)
                    total = int(lag.get("total_lag", 0))
                    if total >= lag_crit:
                        lvl = "CRITICAL"
                    elif total >= lag_warn:
                        lvl = "WARN"
                    else:
                        lvl = "OK"
                    lag_summaries.append({
                        "group": gid,
                        "topic": topic,
                        "total_lag": total,
                        "level": lvl,
                    })
                    if lvl != "OK":
                        signals.append({
                            "level": lvl,
                            "code": "CONSUMER_LAG",
                            "message": f"High lag group='{gid}', topic='{topic}': {total}",
                            "data": {"group": gid, "topic": topic, "total_lag": total},
                        })
                        if rank(lvl) > rank(overall):
                            overall = lvl
        except KafkaError as e:
            signals.append({
                "level": "WARN",
                "code": "LAG_DISCOVERY_FAILED",
                "message": f"Could not enumerate groups/offsets: {e}",
                "data": {},
            })
            if rank("WARN") > rank(overall):
                overall = "WARN"

        return {
            "status": overall,
            "signals": signals,
            "summary": {
                "cluster_id": cid,
                "broker_count": brokers,
                "topics_count": topics_count,
                "under_replicated": urp,
                "offline_partitions": offline,
                "single_broker": single_broker,
                "lag": lag_summaries,
            },
            "timestamp": int(time.time() * 1000),
        }



    def broker_leadership_distribution(self, include_internal: bool = False) -> dict:
        """
        Return how many partitions each broker is the leader for.

        Example Output:
        {
          "total_partitions": int,
          "distribution": { broker_id: count },
          "by_topic": { topic: { broker_id: [partitions...] } }
        }
        """
        try:
            topics_meta = self._admin.describe_topics()
        except Exception as e:
            return {"error": f"describe_topics failed: {e}"}

        distribution = {}
        by_topic = {}
        total = 0

        for t in topics_meta:
            if not include_internal and t.get("is_internal", False):
                continue
            topic_name = t["topic"]
            by_topic[topic_name] = {}
            for p in t.get("partitions", []) or []:
                leader = p.get("leader", -1)
                if leader == -1:
                    continue  # skip offline partitions
                total += 1
                distribution[leader] = distribution.get(leader, 0) + 1
                by_topic[topic_name].setdefault(leader, []).append(p["partition"])

        return {
            "total_partitions": total,
            "distribution": distribution,
            "by_topic": by_topic,
        }

    def get_offline_partitions(self, include_internal: bool = False) -> dict:
        """
        Returns all offline partitions (leader == -1).

        Example Output:
        {
          "offline_count": int,
          "by_topic": { topic: [partition_ids...] }
        }
        """
        try:
            topics_meta = self._admin.describe_topics()
        except Exception as e:
            return {"error": f"describe_topics failed: {e}"}

        result = {"offline_count": 0, "by_topic": {}}

        for t in topics_meta:
            if not include_internal and t.get("is_internal", False):
                continue
            topic_name = t["topic"]
            partitions = t.get("partitions", []) or []
            for p in partitions:
                if p.get("leader", -1) == -1:
                    result["offline_count"] += 1
                    result["by_topic"].setdefault(topic_name, []).append(p["partition"])

        return result


    def get_consumer_lag(self, group_id: str, topic: str, print_output: bool = True) -> Dict[str, Any]:
        """
        Compute lag for one topic in one consumer group.

        Returns:
        {
          "group_id": str,
          "topic": str,
          "by_partition": { <p>: {"end": int, "committed": int|None, "lag": int} },
          "total_lag": int
        }
        """
        result: Dict[str, Any] = {"group_id": group_id, "topic": topic, "by_partition": {}, "total_lag": 0}

        try:
            probe = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,                # we want committed offsets for this group
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=45000,         # must be > session_timeout_ms
                consumer_timeout_ms=5000,
                client_id=f"{self.client_id}-lag",
            )

            parts = probe.partitions_for_topic(topic)
            if not parts:
                if print_output:
                    print(f"[LAG] topic '{topic}' has no partitions (or not found).")
                probe.close()
                return result

            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            end_offsets = probe.end_offsets(tps)

            total = 0
            for tp in tps:
                end = int(end_offsets.get(tp, 0))
                committed = probe.committed(tp)   # None if group never committed on this partition
                lag = end if committed is None else max(end - int(committed), 0)
                result["by_partition"][tp.partition] = {
                    "end": end,
                    "committed": None if committed is None else int(committed),
                    "lag": int(lag),
                }
                total += int(lag)

            result["total_lag"] = total
            probe.close()

            if print_output:
                print(f"[LAG] group='{group_id}' topic='{topic}' total={total}")
                compact = {p: v["lag"] for p, v in result["by_partition"].items()}
                print(f"       by_partition={compact}")
            return result

        except KafkaError as e:
            if print_output:
                print(f"[LAG] failed for group='{group_id}' topic='{topic}': {e}")
            result["error"] = f"KafkaError: {e}"
            return result



    def get_cluster_details(self) -> dict:
        """
        Returns a compact cluster summary useful for analysis:
        - cluster_id, controller_id
        - brokers (node_id, host, port), broker_count
        - topics_count (incl/excl internal), total_partitions
        - consumer_groups_count
        - health signals: under_replicated_count, offline_partition_count,
          topics_with_rf_gt_brokers, single_broker
        """
        from kafka.errors import KafkaError

        summary = {
            "cluster_id": None,
            "controller_id": None,
            "brokers": [],
            "broker_count": 0,
            "topics_count": 0,
            "topics_internal_count": 0,
            "total_partitions": 0,
            "consumer_groups_count": 0,
            "health": {
                "under_replicated_count": 0,
                "offline_partition_count": 0,
                "topics_with_rf_gt_brokers": [],
                "single_broker": False,
            },
        }

        try:
            # --- Core cluster meta
            meta = self._admin.describe_cluster()
            brokers = meta.get("brokers", []) or []
            summary["cluster_id"] = meta.get("cluster_id")
            summary["controller_id"] = meta.get("controller_id")
            summary["brokers"] = [
                {
                    "node_id": b.get("node_id"),
                    "host": b.get("host"),
                    "port": b.get("port"),
                    "rack": b.get("rack"),
                }
                for b in brokers
            ]
            summary["broker_count"] = len(brokers)
            summary["health"]["single_broker"] = (len(brokers) == 1)

            # --- Topics & partitions + health
            topics_meta = self._admin.describe_topics()
            summary["topics_count"] = len(topics_meta)
            internal = 0
            total_partitions = 0
            urp = 0
            offline = 0
            rf_gt_brokers = set()

            for t in topics_meta:
                if t.get("is_internal", False):
                    internal += 1
                parts = t.get("partitions", []) or []
                total_partitions += len(parts)

                # replication factor (assume same across partitions)
                rf = len(parts[0]["replicas"]) if parts else 0
                if summary["broker_count"] and rf > summary["broker_count"]:
                    rf_gt_brokers.add(t["topic"])

                for p in parts:
                    replicas = p.get("replicas", []) or []
                    isr = p.get("isr", []) or []
                    leader = p.get("leader", -1)
                    # Under-replicated when ISR size < replica count
                    if len(isr) < len(replicas):
                        urp += 1
                    # Offline partition when leader is -1
                    if leader == -1:
                        offline += 1

            summary["topics_internal_count"] = internal
            summary["total_partitions"] = total_partitions
            summary["health"]["under_replicated_count"] = urp
            summary["health"]["offline_partition_count"] = offline
            summary["health"]["topics_with_rf_gt_brokers"] = sorted(list(rf_gt_brokers))

            # --- Consumer groups count
            try:
                groups = self._admin.list_consumer_groups()  # [(group_id, protocol_type)]
                summary["consumer_groups_count"] = len(groups)
            except KafkaError:
                # not fatal for cluster summary
                pass

            return summary

        except KafkaError as e:
            return {"error": f"KafkaError: {e}"}
        except Exception as e:
            return {"error": f"Unexpected: {repr(e)}"}




    # ------------------ Public: Topic Details (one-call) ------------------ # Ready
    
    def get_topic_details(self, topic: str) -> Dict[str, Any]:
        """
        Returns a structured dict with:
          - exists: bool
          - metadata: {partitions: int, replication_factor: int, partitions_detail: [{partition, leader, replicas, isr}]}
          - configs: {name: value}
          - watermarks: {partition: {beginning, end, approx_messages}}
        """
        try:
            meta = self._topic_metadata(topic)
            if not meta["exists"]:
                return {
                    "topic": topic,
                    "exists": False,
                    "metadata": None,
                    "configs": None,
                    "watermarks": None,
                }

            cfg = self._topic_configs(topic)
            wmarks = self._topic_watermarks(topic)

            return {
                "topic": topic,
                "exists": True,
                "metadata": meta["metadata"],
                "configs": cfg,
                "watermarks": wmarks,
            }

        except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
            return {
                "topic": topic,
                "exists": None,
                "error": f"{type(e).__name__}: {e}",
                "metadata": None,
                "configs": None,
                "watermarks": None,
            }

    # ------------------ Internal: Metadata ------------------ #
    def _topic_metadata(self, topic: str) -> Dict[str, Any]:
        """
        Uses AdminClient.describe_topics() and extracts:
          partitions, replication_factor, per-partition leader/replicas/isr.
        """
        topics_meta = self._admin.describe_topics()  # all topics
        chosen: Optional[Dict[str, Any]] = None
        for t in topics_meta:
            if t.get("topic") == topic:
                chosen = t
                break

        if not chosen:
            return {"exists": False, "metadata": None}

        parts = chosen.get("partitions", []) or []
        rf = len(parts[0].get("replicas", [])) if parts else 0
        partitions_detail = []
        for p in parts:
            partitions_detail.append(
                {
                    "partition": p.get("partition"),
                    "leader": p.get("leader", -1),
                    "replicas": p.get("replicas", []),
                    "isr": p.get("isr", []),
                }
            )

        return {
            "exists": True,
            "metadata": {
                "partitions": len(parts),
                "replication_factor": rf,
                "partitions_detail": partitions_detail,
            },
        }

    # ------------------ Internal: Configs (robust to protocol tuple shapes) ------------------ #
    def _topic_configs(self, topic: str) -> Dict[str, str]:
        """
        Uses AdminClient.describe_configs([TOPIC]) and normalizes output to {name: value}.
        Handles both 4-tuple and 5-tuple resource responses.
        """
        try:
            responses = self._admin.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic)],
                include_synonyms=False,
            )
        except KafkaError:
            return {}

        cfg: Dict[str, str] = {}
        resp_list = responses if isinstance(responses, list) else [responses]
        for resp in resp_list:
            resources = getattr(resp, "resources", []) or []
            for res in resources:
                # res is often a tuple:
                # (err_code, rtype, rname, configs) OR (err_code, err_msg, rtype, rname, configs)
                err, rname, configs = 0, topic, None
                if isinstance(res, (list, tuple)):
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

                if err:
                    continue
                if not configs:
                    continue

                for entry in configs:
                    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                        name, value = entry[0], entry[1]
                    else:
                        name = getattr(entry, "name", None)
                        value = getattr(entry, "value", None)
                    if name is not None:
                        cfg[str(name)] = None if value is None else str(value)
        return cfg

    # ------------------ Internal: Watermarks ------------------ #
    def _topic_watermarks(self, topic: str) -> Dict[int, Dict[str, Optional[int]]]:
        """
        Uses a lightweight KafkaConsumer to fetch beginning/end offsets per partition.
        Returns {partition: {beginning, end, approx_messages}}
        """
        c = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=None,
            client_id=f"{self.client_id}-wmarks",
            enable_auto_commit=False,
            request_timeout_ms=20000,
            consumer_timeout_ms=5000,
        )
        parts = c.partitions_for_topic(topic)
        if not parts:
            c.close()
            return {}

        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        beginnings = c.beginning_offsets(tps)
        ends = c.end_offsets(tps)

        summary: Dict[int, Dict[str, Optional[int]]] = {}
        for tp in tps:
            b = beginnings.get(tp)
            e = ends.get(tp)
            summary[tp.partition] = {
                "beginning": b,
                "end": e,
                "approx_messages": (e - b) if (b is not None and e is not None) else None,
            }
        c.close()
        return summary


# 2 Ready

    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[Dict[str, str]] = None,
        if_not_exists: bool = True,
        timeout_ms: int = 15000,
    ) -> Dict[str, Any]:
        """
        Create a topic. On your single-broker setup, replication_factor must be 1.
        Returns a structured result with 'created': True/False and 'error' if any.
        """
        try:
            new_topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=configs or {},
            )
            self._admin.create_topics([new_topic], timeout_ms=timeout_ms, validate_only=False)
            result = {"topic": name, "created": True, "error": None}
            print(f"[CREATE] topic='{name}' P={num_partitions} RF={replication_factor}")
            return result
        except TopicAlreadyExistsError:
            if if_not_exists:
                msg = "already exists"
                result = {"topic": name, "created": False, "error": msg}
                print(f"[CREATE] SKIP topic='{name}' ({msg})")
                return result
            raise
        except KafkaError as e:
            result = {"topic": name, "created": False, "error": f"KafkaError: {e}"}
            print(f"[CREATE] FAIL topic='{name}' -> {e}")
            return result
    





# ------------------ Test in main ------------------ #
if __name__ == "__main__":
    # Your Docker Compose exposes OUTSIDE on localhost:9092
    inspector = KafkaInspector(bootstrap_servers="localhost:9092")

# 1 topic meta data -----------
    # topic_name = "my-topic"  # change if needed
    # details = inspector.get_topic_details(topic_name)

    # # Human-readable line
    # if details.get("exists"):
    #     meta = details["metadata"]
    #     print(f"[Topic] {topic_name}  P={meta['partitions']} RF={meta['replication_factor']}")
    # else:
    #     print(f"[Topic] {topic_name} does not exist or cluster unreachable.")

    # # Full JSON (easy to inspect/parse)
    # print(json.dumps(details, indent=2))
    
    # 1 topic meta data -----------

# 2 Create topic -----

#  # Choose a topic that is NOT auto-created by compose to test creation
#     topic_to_create = "demo-topic"
#     # On a single broker, RF must be 1. You can pick multiple partitions if you want.
#     create_result = inspector.create_topic(
#         name=topic_to_create,
#         num_partitions=3,
#         replication_factor=1,
#         configs={"retention.ms": "600000"},  # 10 minutes (example)
#         if_not_exists=True,
#     )

#     # Verify by fetching details
#     details = inspector.get_topic_details(topic_to_create)

#     # Print verification succinctly + full JSON
#     if details.get("exists"):
#         meta = details["metadata"]
#         print(f"[VERIFY] {topic_to_create}  P={meta['partitions']} RF={meta['replication_factor']}")
#     else:
#         print(f"[VERIFY] topic '{topic_to_create}' not found")

#     print(json.dumps({"create_result": create_result, "details": details}, indent=2))
# Create topic -----



#3 cluster details most useful for analysis (controller, brokers, topic/partition stats, consumer groups count, and health signals like offline/under-replicated partitions and RF>brokers): ---------------------------------------------------
# cluster = inspector.get_cluster_details()
#     # Human line
# if "error" not in cluster:
#         print(f"[CLUSTER] id={cluster['cluster_id']} brokers={cluster['broker_count']} "
#               f"controller={cluster['controller_id']} topics={cluster['topics_count']} "
#               f"parts={cluster['total_partitions']} groups={cluster['consumer_groups_count']}")
# else:
#         print(cluster["error"])

#     # Full JSON
# print(json.dumps(cluster, indent=2))
# --------------------------------------------------------

#4 get_consumer_lag ---------
    # group_id = "lag-demo"
    # topic = "my-topic"

    # lag = inspector.get_consumer_lag(group_id=group_id, topic=topic, print_output=True)

    # # Also dump full JSON (handy for verification)
    # print(json.dumps(lag, indent=2))
    
# get_consumer_lag ---------

# 5  get_offline_partitions ----

# offline = inspector.get_offline_partitions()
# print(f"[OFFLINE] count={offline.get('offline_count')}")
# print(json.dumps(offline, indent=2))

# ----------------

# 6 ----
    dist = inspector.broker_leadership_distribution()
    # print("[LEADERSHIP DISTRIBUTION]")
    # print(json.dumps(dist, indent=2))
#  ---------

#  7 alert summaries  -----------------------------


# summary = inspector.alert_summary()
# print(f"[ALERT] status={summary['status']}")
# print(json.dumps(summary, indent=2))

# -----------------------------

    # topics = inspector.list_topics()  # default: exclude internal
    # print("[TOPICS] (exclude internal):", topics)

    # topics_all = inspector.list_topics(include_internal=True)
    # print("[TOPICS] (include internal):", topics_all)