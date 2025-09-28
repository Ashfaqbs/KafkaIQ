import sys
import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import datetime
from fastmcp import FastMCP

# Kafka imports
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError, TopicAlreadyExistsError
from typing import Union, List, Dict
import logging

# mail imports
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# Initialize MCP server
mcp = FastMCP("Kafka MCP Server")




class KafkaInspector:
    """
    Holds broker details (object-level) and exposes single-responsibility utilities.
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        client_id: str = "kafka-inspector",
        request_timeout_ms: int = 45000,  # keep > session_timeout_ms
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
    
    def get_topic_config(self, topic_name: str) -> Dict[str, str]:
        """
        Get configuration for a specific topic.
        """
        try:
            # Create ConfigResource for TOPIC type
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            
            # Get configurations
            configs_result = self._admin.describe_configs([resource])
            
            # Extract configuration values
            config_dict: Dict[str, str] = {}
            if resource in configs_result:
                config_entries = configs_result[resource]
                for config_name, config_entry in config_entries.items():
                    # Handle different kafka-python versions
                    if hasattr(config_entry, "value"):
                        config_dict[config_name] = config_entry.value
                    else:
                        # Fallback for older versions
                        config_dict[config_name] = str(config_entry)
            
            return config_dict
            
        except Exception as e:
            logger.error(f"[GET_TOPIC_CONFIG] failed for topic {topic_name}: {e}")
            # Check if topic exists first
            try:
                topics = self.list_topics(include_internal=True)
                if topic_name not in topics:
                    logger.error(f"Topic '{topic_name}' does not exist")
                    return {"error": f"Topic '{topic_name}' does not exist"}
            except Exception:
                pass
            
            return {"error": f"Failed to retrieve config: {str(e)}"}

    
    
    
        
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
            logger.error(f"[LIST_TOPICS] failed: {e}")
            return []
    
    def describe_topic(self, topic_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific topic.
        """
        try:
            topics_meta = self._admin.describe_topics([topic_name])
            if topics_meta:
                return topics_meta[0]
            return {}
        except KafkaError as e:
            logger.error(f"[DESCRIBE_TOPIC] failed for topic {topic_name}: {e}")
            return {}
    
    def create_topic(
        self, 
        topic_name: str, 
        num_partitions: int = 1, 
        replication_factor: int = 1,
        topic_configs: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a new topic.
        """
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=topic_configs or {}
            )
            result = self._admin.create_topics([topic])
            # Wait for the operation to complete
            for topic_name, future in result.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"Topic {topic_name} created successfully")
                    return True
                except TopicAlreadyExistsError:
                    logger.warning(f"Topic {topic_name} already exists")
                    return False
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    return False
        except KafkaError as e:
            logger.error(f"[CREATE_TOPIC] failed: {e}")
            return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a topic.
        """
        try:
            result = self._admin.delete_topics([topic_name])
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic_name} deleted successfully")
                    return True
                except Exception as e:
                    logger.error(f"Failed to delete topic {topic_name}: {e}")
                    return False
        except KafkaError as e:
            logger.error(f"[DELETE_TOPIC] failed: {e}")
            return False
    
    def get_topic_config(self, topic_name: str) -> Dict[str, str]:
        """
        Get configuration for a specific topic.
        """
        try:
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = self._admin.describe_configs([resource])
            if resource in configs:
                return {config.name: config.value for config in configs[resource].values()}
            return {}
        except KafkaError as e:
            logger.error(f"[GET_TOPIC_CONFIG] failed for topic {topic_name}: {e}")
            return {}
    
    def get_cluster_details(self) -> Dict[str, Any]:
        """
        Get comprehensive cluster health and metadata information.
        """
        try:
            import time
            
            # Get cluster metadata
            metadata = self._admin._client.cluster
            cluster_id = getattr(metadata, 'cluster_id', 'unknown')
            
            # Get broker information
            brokers = list(metadata.brokers())
            broker_count = len(brokers)
            
            # Get topic information
            topics = self.list_topics(include_internal=False)
            topics_count = len(topics)
            
            # Check partition health
            offline_partition_count = 0
            under_replicated_count = 0
            total_partitions = 0
            
            try:
                # Get detailed topic metadata for health check
                all_topics_meta = self._admin.describe_topics()
                for topic_meta in all_topics_meta:
                    if topic_meta.get("is_internal", False):
                        continue
                    
                    partitions = topic_meta.get("partitions", [])
                    total_partitions += len(partitions)
                    
                    for partition in partitions:
                        # Check if partition is offline (no leader)
                        if partition.get("leader") is None or partition.get("leader") == -1:
                            offline_partition_count += 1
                        
                        # Check if partition is under-replicated
                        replicas = partition.get("replicas", [])
                        isr = partition.get("isr", [])  # in-sync replicas
                        if len(isr) < len(replicas):
                            under_replicated_count += 1
            
            except Exception as e:
                logger.warning(f"Could not get detailed partition health: {e}")
            
            # Determine if single broker setup
            single_broker = broker_count == 1
            
            return {
                "cluster_id": cluster_id,
                "broker_count": broker_count,
                "brokers": [{"id": b.nodeId, "host": b.host, "port": b.port} for b in brokers],
                "topics_count": topics_count,
                "total_partitions": total_partitions,
                "health": {
                    "offline_partition_count": offline_partition_count,
                    "under_replicated_count": under_replicated_count,
                    "single_broker": single_broker
                },
                "timestamp": int(time.time() * 1000)
            }
            
        except Exception as e:
            logger.error(f"[GET_CLUSTER_DETAILS] failed: {e}")
            return {"error": f"Failed to get cluster details: {str(e)}"}
    
    def get_consumer_lag(self, group_id: str, topic_name: str, print_output: bool = True) -> Dict[str, Any]:
        """
        Get consumer lag information for a specific group and topic.
        """
        consumer = None
        try:
            # Get consumer group offsets
            offsets = self._admin.list_consumer_group_offsets(group_id)
            
            # Filter for the specific topic
            topic_partitions = [tp for tp in offsets.keys() if tp.topic == topic_name]
            
            if not topic_partitions:
                return {"total_lag": 0, "partitions": [], "message": f"No offsets found for topic {topic_name}"}
            
            # Get high water marks using a single consumer with proper config
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-lag-check",
                group_id=None,  # Don't join a consumer group
                enable_auto_commit=False,  # Don't auto-commit
                consumer_timeout_ms=5000,  # 5 second timeout
                api_version_auto_timeout_ms=3000,  # Quick API version detection
                request_timeout_ms=10000,  # 10 second request timeout
                metadata_max_age_ms=30000,  # Refresh metadata every 30 seconds
                auto_offset_reset='latest'
            )
            
            partition_lags = []
            total_lag = 0
            
            # Get all high water marks at once for efficiency
            all_partitions = list(topic_partitions)
            consumer.assign(all_partitions)
            
            # Get end offsets for all partitions at once
            end_offsets = consumer.end_offsets(all_partitions)
            
            for tp in topic_partitions:
                try:
                    # Get consumer offset
                    consumer_offset = offsets[tp].offset if offsets[tp] else 0
                    
                    # Get high water mark from end_offsets
                    high_water_mark = end_offsets.get(tp, 0)
                    
                    # Calculate lag
                    lag = max(0, high_water_mark - consumer_offset)
                    total_lag += lag
                    
                    partition_lags.append({
                        "partition": tp.partition,
                        "consumer_offset": consumer_offset,
                        "high_water_mark": high_water_mark,
                        "lag": lag
                    })
                    
                except Exception as e:
                    logger.warning(f"Could not get lag for partition {tp.partition}: {e}")
                    partition_lags.append({
                        "partition": tp.partition,
                        "consumer_offset": 0,
                        "high_water_mark": 0,
                        "lag": 0,
                        "error": str(e)
                    })
            
            result = {
                "group_id": group_id,
                "topic": topic_name,
                "total_lag": total_lag,
                "partitions": partition_lags
            }
            
            if print_output:
                logger.info(f"Consumer lag for group '{group_id}', topic '{topic_name}': {total_lag}")
            
            return result
            
        except Exception as e:
            logger.error(f"[GET_CONSUMER_LAG] failed: {e}")
            return {"error": f"Failed to get consumer lag: {str(e)}", "total_lag": 0}
        finally:
            # Always close the consumer
            if consumer:
                try:
                    consumer.close()
                except Exception as e:
                    logger.warning(f"Error closing consumer: {e}")
    
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
            
            logger.info(f"Found {len(group_ids)} consumer groups: {group_ids}")
            
            for gid in group_ids:
                try:
                    # Add timeout protection for each group
                    offsets = self._admin.list_consumer_group_offsets(gid)  # {TopicPartition: OffsetAndMetadata}
                    topics_for_group = sorted({tp.topic for tp in offsets.keys()})
                    
                    logger.info(f"Group '{gid}' consumes topics: {topics_for_group}")
                    
                    for topic in topics_for_group:
                        try:
                            lag = self.get_consumer_lag(gid, topic, print_output=False)
                            
                            # Handle error case
                            if "error" in lag:
                                logger.warning(f"Failed to get lag for group='{gid}', topic='{topic}': {lag['error']}")
                                total = 0
                            else:
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
                        except Exception as topic_e:
                            logger.warning(f"Failed to process lag for group='{gid}', topic='{topic}': {topic_e}")
                            lag_summaries.append({
                                "group": gid,
                                "topic": topic,
                                "total_lag": 0,
                                "level": "UNKNOWN",
                                "error": str(topic_e)
                            })
                            
                except Exception as group_e:
                    logger.warning(f"Failed to process consumer group '{gid}': {group_e}")
                    signals.append({
                        "level": "WARN",
                        "code": "GROUP_PROCESSING_FAILED",
                        "message": f"Could not process consumer group '{gid}': {group_e}",
                        "data": {"group": gid},
                    })
                    if rank("WARN") > rank(overall):
                        overall = "WARN"
                        
        except KafkaError as e:
            signals.append({
                "level": "WARN",
                "code": "LAG_DISCOVERY_FAILED",
                "message": f"Could not enumerate groups/offsets: {e}",
                "data": {},
            })
            if rank("WARN") > rank(overall):
                overall = "WARN"
        except Exception as e:
            logger.error(f"Unexpected error in lag discovery: {e}")
            signals.append({
                "level": "WARN",
                "code": "LAG_DISCOVERY_ERROR",
                "message": f"Unexpected error during lag discovery: {e}",
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

# Global Kafka inspector instance
kafka_inspector = None

# --- MCP Tools ---

@mcp.tool
def initialize_kafka_connection(
    bootstrap_servers: str,
    client_id: str = "kafka-mcp-client",
    request_timeout_ms: int = 45000,
    api_version_auto_timeout_ms: int = 8000
) -> str:
    """
    Initialize connection to Kafka cluster.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
        client_id: Client identifier for this connection
        request_timeout_ms: Request timeout in milliseconds
        api_version_auto_timeout_ms: API version auto-detection timeout in milliseconds
    
    Returns:
        Status message indicating success or failure
    """
    global kafka_inspector
    try:
        servers = [s.strip() for s in bootstrap_servers.split(',')]
        kafka_inspector = KafkaInspector(
            bootstrap_servers=servers,
            client_id=client_id,
            request_timeout_ms=request_timeout_ms,
            api_version_auto_timeout_ms=api_version_auto_timeout_ms
        )
        return f"Successfully connected to Kafka cluster at {bootstrap_servers}"
    except Exception as e:
        return f"Failed to connect to Kafka cluster: {str(e)}"

@mcp.tool
def list_kafka_topics(include_internal: bool = False) -> str:
    """
    List all topics in the Kafka cluster.
    
    Args:
        include_internal: Whether to include internal topics like __consumer_offsets
    
    Returns:
        JSON string containing list of topic names
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        topics = kafka_inspector.list_topics(include_internal=include_internal)
        return json.dumps({
            "topics": topics,
            "count": len(topics),
            "include_internal": include_internal
        })
    except Exception as e:
        return json.dumps({"error": f"Failed to list topics: {str(e)}"})

@mcp.tool
def describe_kafka_topic(topic_name: str) -> str:
    """
    Get detailed information about a specific Kafka topic.
    
    Args:
        topic_name: Name of the topic to describe
    
    Returns:
        JSON string containing topic metadata
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        topic_info = kafka_inspector.describe_topic(topic_name)
        return json.dumps(topic_info, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to describe topic {topic_name}: {str(e)}"})

@mcp.tool
def create_kafka_topic(
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    topic_configs: str = "{}"
) -> str:
    """
    Create a new Kafka topic.
    
    Args:
        topic_name: Name of the topic to create
        num_partitions: Number of partitions for the topic
        replication_factor: Replication factor for the topic
        topic_configs: JSON string of topic configurations
    
    Returns:
        Status message indicating success or failure
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        configs = json.loads(topic_configs) if topic_configs != "{}" else None
        success = kafka_inspector.create_topic(
            topic_name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=configs
        )
        if success:
            return json.dumps({
                "status": "success",
                "message": f"Topic '{topic_name}' created successfully",
                "topic_name": topic_name,
                "num_partitions": num_partitions,
                "replication_factor": replication_factor
            })
        else:
            return json.dumps({
                "status": "failed",
                "message": f"Failed to create topic '{topic_name}'"
            })
    except Exception as e:
        return json.dumps({"error": f"Failed to create topic: {str(e)}"})

@mcp.tool
def delete_kafka_topic(topic_name: str) -> str:
    """
    Delete a Kafka topic.
    
    Args:
        topic_name: Name of the topic to delete
    
    Returns:
        Status message indicating success or failure
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        success = kafka_inspector.delete_topic(topic_name)
        if success:
            return json.dumps({
                "status": "success",
                "message": f"Topic '{topic_name}' deleted successfully"
            })
        else:
            return json.dumps({
                "status": "failed",
                "message": f"Failed to delete topic '{topic_name}'"
            })
    except Exception as e:
        return json.dumps({"error": f"Failed to delete topic: {str(e)}"})

@mcp.tool
def get_kafka_topic_config(topic_name: str) -> str:
    """
    Get configuration settings for a specific Kafka topic.
    
    Args:
        topic_name: Name of the topic to get configuration for
    
    Returns:
        JSON string containing topic configuration
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        config = kafka_inspector.get_topic_config(topic_name)
        
        # Handle empty config case
        if not config:
            return json.dumps({
                "topic_name": topic_name,
                "configuration": {},
                "message": f"No configuration found for topic '{topic_name}' or topic does not exist"
            }, indent=2)
        
        return json.dumps({
            "topic_name": topic_name,
            "configuration": config,
            "config_count": len(config)
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error in get_kafka_topic_config: {str(e)}")
        return json.dumps({
            "error": f"Failed to get topic config: {str(e)}",
            "topic_name": topic_name,
            "suggestion": "Check if topic exists and you have proper permissions"
        })

@mcp.tool
def kafka_health_check() -> str:
    """
    Check the health status of the Kafka connection.
    
    Returns:
        JSON string containing health status information
    """
    if kafka_inspector is None:
        return json.dumps({
            "status": "disconnected",
            "message": "Kafka connection not initialized"
        })
    
    try:
        # Try to list topics as a health check
        topics = kafka_inspector.list_topics()
        return json.dumps({
            "status": "healthy",
            "message": "Kafka connection is working",
            "bootstrap_servers": kafka_inspector.bootstrap_servers,
            "client_id": kafka_inspector.client_id,
            "topics_count": len(topics)
        })
    except Exception as e:
        return json.dumps({
            "status": "unhealthy",
            "message": f"Kafka connection error: {str(e)}"
        })

@mcp.tool
def get_cluster_details() -> str:
    """
    Get comprehensive cluster health and metadata information.
    
    Returns:
        JSON string containing detailed cluster information including broker count, topics, and partition health
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        cluster_info = kafka_inspector.get_cluster_details()
        return json.dumps(cluster_info, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to get cluster details: {str(e)}"})

@mcp.tool
def get_consumer_lag(group_id: str, topic_name: str) -> str:
    """
    Get consumer lag information for a specific consumer group and topic.
    
    Args:
        group_id: Consumer group ID to check
        topic_name: Topic name to analyze lag for
    
    Returns:
        JSON string containing lag information per partition and total lag
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        lag_info = kafka_inspector.get_consumer_lag(group_id, topic_name, print_output=False)
        return json.dumps(lag_info, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to get consumer lag: {str(e)}"})

@mcp.tool
def kafka_alert_summary() -> str:
    """
    Get comprehensive cluster health alert summary with automatic threshold-based analysis.
    
    Returns:
        JSON string containing overall status (OK/WARN/CRITICAL), detected issues, and detailed metrics.
        Automatically analyzes:
        - Offline partitions
        - Under-replicated partitions  
        - Consumer lag across all groups and topics
        - Single broker warnings
    """
    if kafka_inspector is None:
        return json.dumps({
            "status": "CRITICAL",
            "error": "Kafka connection not initialized. Please call initialize_kafka_connection first."
        })
    
    try:
        alert_summary = kafka_inspector.alert_summary()
        return json.dumps(alert_summary, indent=2)
    except Exception as e:
        return json.dumps({
            "status": "CRITICAL", 
            "error": f"Failed to generate alert summary: {str(e)}"
        })


@mcp.tool
def get_broker_resources(broker_id: str) -> str:
    """
    Get resource information for a specific Kafka broker.
    
    Args:
        broker_id: Broker ID or hostname to get resource information for
    
    Returns:
        JSON string containing broker resource details (RAM, storage, cores)
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        # TODO: Implement actual resource monitoring integration
        # This would typically connect(external sys) to:
        # - Prometheus/Grafana for metrics
        # - JMX for Kafka-specific metrics  
        # - System monitoring tools (htop, iostat, etc.)
        # - Cloud provider APIs (AWS CloudWatch, etc.)
        # - Kubernetes metrics if running in K8s
        
        # For now, return hardcoded realistic values
        broker_resources = {
            "ram_gb": 32,
            "storage_gb": 1000, 
            "cores": 8
        }
        
        return json.dumps({
            "broker_id": broker_id,
            "resources": broker_resources,
            "status": "active",
            "note": "Hardcoded values - integrate with monitoring system"
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "error": f"Failed to get broker resources: {str(e)}",
            "broker_id": broker_id
        })

@mcp.tool 
def send_email_notification(recipient_email: str, content: str, subject: str = "KafkaIQ Notification") -> str:
    """
    Send email notification using Gmail SMTP.
    
    Args:
        recipient_email: Email address to send notification to
        content: Email content/body
        subject: Email subject line (optional)
    
    Returns:
        JSON string indicating email send status
    """
    try:
        # Validate inputs first
        if not recipient_email or "@" not in recipient_email:
            return json.dumps({
                "status": "failed",
                "error": "Invalid recipient email address"
            })
        
        if not content.strip():
            return json.dumps({
                "status": "failed",
                "error": "Email content cannot be empty"
            })
        
        # Gmail SMTP settings
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        
        # Email credentials or get from env
        sender_email = 'mail'
        sender_password = 'pp'
        
        # Create email message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject
        
        # Simple text email body
        email_body = f"""KafkaIQ Notification
===================

{content}

---
Sent by KafkaIQ at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        message.attach(MIMEText(email_body, "plain"))
        
        # Send email via Gmail SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Enable encryption
        server.login(sender_email, sender_password)
        
        # Send the email
        text = message.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        return json.dumps({
            "status": "sent",
            "message": "Email sent successfully",
            "recipient": recipient_email,
            "subject": subject,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except smtplib.SMTPAuthenticationError:
        return json.dumps({
            "status": "failed",
            "error": "Gmail authentication failed. Check email/password or enable 2FA and use App Password"
        })
    except Exception as e:
        return json.dumps({
            "status": "failed",
            "error": f"Failed to send email: {str(e)}"
        })
        
        
@mcp.tool
def broker_leadership_distribution(include_internal: bool = False) -> str:
    """
    Return how many partitions each broker is the leader for.

    Example Output:
    {
      "total_partitions": int,
      "distribution": { broker_id: count },
      "by_topic": { topic: { broker_id: [partitions...] } }
    }
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        topics_meta = kafka_inspector._admin.describe_topics()
    except Exception as e:
        return json.dumps({"error": f"describe_topics failed: {e}"})
    
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
    
    result = {
        "total_partitions": total,
        "distribution": distribution,
        "by_topic": by_topic,
    }
    
    return json.dumps(result, indent=2)

@mcp.tool
def get_offline_partitions(include_internal: bool = False) -> str:
    """
    Returns all offline partitions (leader == -1).

    Example Output:
    {
      "offline_count": int,
      "by_topic": { topic: [partition_ids...] }
    }
    """
    if kafka_inspector is None:
        return json.dumps({"error": "Kafka connection not initialized. Please call initialize_kafka_connection first."})
    
    try:
        topics_meta = kafka_inspector._admin.describe_topics()
    except Exception as e:
        return json.dumps({"error": f"describe_topics failed: {e}"})
    
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
    
    return json.dumps(result, indent=2)

        
        

# --- Main entrypoint ---
def main():
    try:
        logger.info("Starting Kafka MCP server...")
        logger.info("Available tools:")
        logger.info("  - initialize_kafka_connection: Connect to Kafka cluster")
        logger.info("  - list_kafka_topics: List all topics")
        logger.info("  - describe_kafka_topic: Get topic details")
        logger.info("  - create_kafka_topic: Create new topic")
        logger.info("  - delete_kafka_topic: Delete topic")
        logger.info("  - get_kafka_topic_config: Get topic configuration")
        logger.info("  - kafka_health_check: Check connection health")
        logger.info("  - get_cluster_details: Get comprehensive cluster information")
        logger.info("  - get_consumer_lag: Analyze consumer lag for specific group/topic")
        logger.info("  - kafka_alert_summary: Get intelligent cluster health analysis")
        
        mcp.run(transport="streamable-http", host="127.0.0.1", port=8080)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()