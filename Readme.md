# KafkaIQ 🧠⚡

**AI-Powered Kafka Management via Model Control Protocol (MCP)**

KafkaIQ is an intelligent Kafka management tool that exposes Apache Kafka operations through the Model Control Protocol (MCP), enabling AI assistants to seamlessly interact with and manage Kafka clusters through natural language commands.

## 🚀 Overview

Transform your Kafka operations with the power of AI. KafkaIQ acts as an intelligent middleware layer between AI systems and Kafka infrastructure, translating natural language requests into precise Kafka operations.

```
AI Assistant → MCP Protocol → KafkaIQ → Apache Kafka Cluster
```

## ✨ Features

- **🤖 AI-Native Integration**: Built specifically for AI assistants using MCP protocol
- **⚡ Real-time Operations**: Instant topic management, configuration, and monitoring
- **🔍 Intelligent Inspection**: Smart cluster analysis and health monitoring
- **📊 Advanced Analytics**: Consumer lag analysis and partition leadership tracking
- **🛡️ Proactive Alerting**: Automated health checks with CRITICAL/WARN/OK status
- **📧 Email Notifications**: Built-in Gmail SMTP integration for alerts
- **🌐 HTTP API**: RESTful streamable HTTP interface for easy integration

## 🛠️ Available Tools

| Tool | Description | Example Usage |
|------|-------------|---------------|
| `initialize_kafka_connection` | Connect to Kafka cluster | Connect to localhost:9092 |
| `list_kafka_topics` | List all topics | Show me all topics |
| `describe_kafka_topic` | Get topic details | Describe the 'orders' topic |
| `create_kafka_topic` | Create new topic | Create topic 'events' with 3 partitions |
| `delete_kafka_topic` | Delete topic | Remove the 'old-logs' topic |
| `get_kafka_topic_config` | Get topic configuration | Show config for 'user-data' |
| `kafka_health_check` | Check connection health | Is Kafka healthy? |
| `get_cluster_details` | Get cluster information | Show cluster overview |
| `get_consumer_lag` | Analyze consumer lag | Check lag for group 'processors' |
| `kafka_alert_summary` | Intelligent health analysis | Give me cluster health status |
| `broker_leadership_distribution` | Partition leadership analysis | Show broker partition distribution |
| `get_offline_partitions` | Find offline partitions | List all offline partitions |
| `get_broker_resources` | Get broker resources | Show resources for broker-1 |
| `send_email_notification` | Send email alerts | Email cluster status to admin |

## 📦 Installation

### Prerequisites
- Python 3.8+
- Apache Kafka cluster
- Gmail account (for email notifications)

### Setup
```bash
# Clone the repository
git clone https://github.com/ashfaqbs/kafkaiq.git
cd kafkaiq

# Install dependencies
pip install fastmcp kafka-python

# Set email credentials (optional)
export KAFKAIQ_GMAIL_USER="your-email@gmail.com"
export KAFKAIQ_GMAIL_PASSWORD="your-app-password"

# Run the server
python kafka_mcp_server.py
```

## 🚀 Quick Start

### 1. Start KafkaIQ Server
```bash
python kafka_mcp_server.py
```
Server starts on `http://127.0.0.1:8080`

### 2. Connect to Your AI Assistant
Configure your AI assistant to use the MCP endpoint at `http://127.0.0.1:8080`

### 3. Start Managing Kafka with Natural Language!

## 💬 Example Conversations

### Basic Operations

**Connection Setup:**
```
Human: Connect to my local Kafka cluster server : localhost:9092
AI → KafkaIQ: initialize_kafka_connection("localhost:9092")
KafkaIQ Response: "Successfully connected to Kafka cluster at localhost:9092"
AI → Human: "Connected to your Kafka cluster successfully!"
```

**Topic Management:**
```
Human: What topics exist in my cluster?
AI → KafkaIQ: list_kafka_topics()
KafkaIQ Response: {
  "topics": ["orders", "users", "events", "payments"],
  "count": 4,
  "include_internal": false
}
AI → Human: "Your cluster has 4 topics: orders, users, events, and payments."
```

**Create New Topic:**
```
Human: Create a topic called 'notifications' with 5 partitions
AI → KafkaIQ: create_kafka_topic("notifications", 5, 1)
KafkaIQ Response: {
  "status": "success",
  "message": "Topic 'notifications' created successfully",
  "topic_name": "notifications",
  "num_partitions": 5,
  "replication_factor": 1
}
AI → Human: "Successfully created topic 'notifications' with 5 partitions!"
```

### Health Monitoring

**Cluster Health Check:**
```
Human: How is my Kafka cluster doing?
AI → KafkaIQ: kafka_alert_summary()
KafkaIQ Response: {
  "status": "OK",
  "signals": [],
  "summary": {
    "cluster_id": "kafka-cluster-1",
    "broker_count": 3,
    "topics_count": 15,
    "under_replicated": 0,
    "offline_partitions": 0,
    "single_broker": false,
    "lag": [
      {"group": "processors", "topic": "orders", "total_lag": 50, "level": "OK"},
      {"group": "analytics", "topic": "events", "total_lag": 1250, "level": "OK"}
    ]
  }
}
AI → Human: "Your Kafka cluster is healthy! 3 brokers running, 15 topics, no offline partitions or under-replicated partitions. Consumer lag looks good across all groups."
```

**Consumer Lag Analysis:**
```
Human: Check consumer lag for the 'analytics' group on 'events' topic
AI → KafkaIQ: get_consumer_lag("analytics", "events")
KafkaIQ Response: {
  "group_id": "analytics",
  "topic": "events",
  "total_lag": 1250,
  "partitions": [
    {"partition": 0, "consumer_offset": 10000, "high_water_mark": 10500, "lag": 500},
    {"partition": 1, "consumer_offset": 8000, "high_water_mark": 8750, "lag": 750}
  ]
}
AI → Human: "The 'analytics' group has a total lag of 1,250 messages on the 'events' topic. Partition 0 has 500 messages lag, partition 1 has 750 messages lag."
```

### Advanced Analytics

**Partition Leadership:**
```
Human: Show me how partitions are distributed across brokers
AI → KafkaIQ: broker_leadership_distribution()
KafkaIQ Response: {
  "total_partitions": 45,
  "distribution": {
    "1": 15,
    "2": 15, 
    "3": 15
  },
  "by_topic": {
    "orders": {"1": [0, 3], "2": [1, 4], "3": [2, 5]},
    "events": {"1": [0, 2], "2": [1, 3], "3": [4, 5]}
  }
}
AI → Human: "Partition leadership is well-balanced! Each of your 3 brokers leads exactly 15 partitions out of 45 total. The distribution looks optimal."
```

**Offline Partitions Check:**
```
Human: Are there any offline partitions?
AI → KafkaIQ: get_offline_partitions()
KafkaIQ Response: {
  "offline_count": 0,
  "by_topic": {}
}
AI → Human: "Great news! There are no offline partitions in your cluster. All partitions have active leaders."
```

### Resource Monitoring

**Broker Resources:**
```
Human: What are the resources for broker-1?
AI → KafkaIQ: get_broker_resources("broker-1")
KafkaIQ Response: {
  "broker_id": "broker-1",
  "resources": {
    "ram_gb": 32,
    "storage_gb": 1000,
    "cores": 8
  },
  "status": "active",
  "note": "Hardcoded values - integrate with monitoring system"
}
AI → Human: "Broker-1 has 32GB RAM, 1TB storage, and 8 CPU cores. Status is active."
```

### Email Notifications

**Send Alert:**
```
Human: Email the cluster status to admin@company.com
AI → KafkaIQ: send_email_notification("admin@company.com", "Cluster Status: OK\nBrokers: 3\nTopics: 15\nNo issues detected", "KafkaIQ Daily Report")
KafkaIQ Response: {
  "status": "sent",
  "message": "Email sent successfully",
  "recipient": "admin@company.com",
  "subject": "KafkaIQ Daily Report",
  "timestamp": "2025-09-28T10:30:00"
}
AI → Human: "Successfully sent the cluster status report to admin@company.com!"
```

## 🎯 Use Cases

### DevOps & Infrastructure Management
- **Automated Monitoring**: "Alert me if any partitions go offline"
- **Health Dashboards**: "Generate daily cluster health reports"
- **Incident Response**: "What's causing the high consumer lag?"
- **Capacity Planning**: "Show me broker resource utilization"

### Data Engineering
- **Pipeline Management**: "Create topics for new data streams"
- **Performance Tuning**: "Which consumers are lagging behind?"
- **Data Quality**: "Monitor topic configurations for compliance"
- **Stream Processing**: "Set up topics for real-time analytics"

### Development Workflows
- **Environment Setup**: "Create development topics with proper configs"
- **Testing**: "Clean up test topics after integration tests"
- **Debugging**: "Why are messages not being consumed?"
- **Configuration Management**: "Backup topic configurations"

### Business Intelligence
- **Operational Insights**: "Generate weekly Kafka usage reports"
- **Cost Optimization**: "Identify underutilized topics"
- **SLA Monitoring**: "Track consumer lag against SLAs"
- **Trend Analysis**: "Show partition distribution trends"

## 🏗️ Architecture

### Core Components

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   AI Assistant  │    │     KafkaIQ      │    │  Kafka Cluster  │
│                 │◄──►│   MCP Server     │◄──►│                 │
│ (Claude, GPT)   │    │   (Port 8080)    │    │  (Brokers)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         │                        │                       │
         ▼                        ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Natural Language│    │  Tool Execution  │    │   Operations    │
│   Commands      │    │   & Response     │    │  & Monitoring   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Key Technologies
- **FastMCP**: Model Control Protocol implementation
- **kafka-python**: Kafka client library
- **Gmail SMTP**: Email notification system
- **JSON**: Structured data exchange
- **HTTP/REST**: Communication protocol

## 🔧 Configuration

### Environment Variables
```bash
# Email Configuration (Optional)
KAFKAIQ_GMAIL_USER=your-email@gmail.com
KAFKAIQ_GMAIL_PASSWORD=your-app-password

```

### Alert Thresholds
Customize alert thresholds in `kafka_inspector.alert_summary()`:
```python
# Built-in thresholds (edit as needed)
offline_crit = 1              # Any offline partition = CRITICAL
urp_warn, urp_crit = 1, 5     # Under-replicated partitions
lag_warn, lag_crit = 10_000, 100_000  # Consumer lag thresholds
single_broker_warn = True     # Warn on single broker setup
```

## 🔐 Security

### Authentication
- **Kafka**: Supports SASL/PLAIN, SASL/SCRAM, SSL/TLS
- **Email**: Gmail App Passwords (2FA required)
- **MCP**: HTTP-based with potential for API key integration

### Best Practices
- Use environment variables for credentials
- Enable Kafka SSL/TLS in production
- Implement network-level access controls
- Regular credential rotation
- Monitor access logs

## 📊 Monitoring & Observability

### Built-in Metrics
- Cluster health status (OK/WARN/CRITICAL)
- Partition leadership distribution
- Consumer lag across all groups
- Offline partition detection
- Under-replicated partition monitoring

### Integration Points
- **Prometheus**: Metrics export (future enhancement)
- **Grafana**: Dashboard integration
- **Slack/Teams**: Alert forwarding
- **PagerDuty**: Incident management
- **SIEM**: Security event logging

## 🤝 AI Assistant Integration

### Supported Platforms
- **Claude** (Anthropic) - Primary development platform
- **ChatGPT** (OpenAI) - Compatible with MCP
- **Custom AI Systems** - Any MCP-compatible assistant

### Integration Steps
1. Start KafkaIQ server
2. Configure AI assistant with MCP endpoint
3. Test connection with basic commands
4. Set up monitoring and alerting workflows

## 🚀 Advanced Usage

### Batch Operations
```
Human: Create 5 topics for microservices: user-service, order-service, payment-service, notification-service, audit-service
AI: I'll create all 5 topics for your microservices architecture...
```

### Automated Workflows
```
Human: Set up monitoring that emails me if any consumer lag exceeds 50,000 messages
AI: I'll configure automated monitoring with email alerts for high consumer lag...
```

### Custom Configurations
```python
# Custom topic configuration example
topic_configs = {
    "retention.ms": "604800000",  # 7 days
    "compression.type": "lz4",
    "max.message.bytes": "1048576"  # 1MB
}
```

## 🐛 Troubleshooting

### Common Issues

**Connection Failed**
```bash
Error: "Failed to connect to Kafka cluster"
Solution: Check if Kafka is running on specified port
```

**Email Not Sending**
```bash
Error: "Gmail authentication failed"
Solution: Enable 2FA and generate App Password
```

**Consumer Lag Timeout**
```bash
Error: "Consumer lag check timed out"
Solution: Increase timeout in KafkaConsumer config
```

**Permission Denied**
```bash
Error: "Not authorized to perform operation"
Solution: Check Kafka ACLs and user permissions
```

## 🤝 Contributing

We welcome contributions!

### Development Setup
```bash
# Clone and setup development environment
git clone https://github.com/yourusername/kafkaiq.git
cd kafkaiq

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install development dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Start development server
python kafka_mcp_server.py
```

### Contribution Areas
- 🐛 Bug fixes and stability improvements
- ✨ New tool implementations
- 📚 Documentation and examples
- 🧪 Test coverage expansion
- 🔧 Performance optimizations

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **FastMCP Team**: For the excellent MCP framework
- **Kafka Community**: For the robust kafka-python library
- **Anthropic**: For Claude and MCP protocol development

## 📞 Support & Community

- 🐛 **Issues**: [GitHub Issues](https://github.com/ashfaqbs/kafkaiq/issues)
- 💡 **Discussions**: [GitHub Discussions](https://github.com/ashfaqbs/kafkaiq/discussions)

## 🏷️ Tags

`kafka` `ai` `mcp` `devops` `monitoring` `alerts` `data-engineering` `stream-processing` `automation` `python` `fastmcp` `claude` `chatgpt` `infrastructure`

---

**Made with ❤️ for the AI and Data Engineering communities**

*Transform your Kafka operations with the power of AI - Experience KafkaIQ today!*