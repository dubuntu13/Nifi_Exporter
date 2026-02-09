# NiFi Cluster Exporter for Prometheus
Hi!

I wrote a lightweight Prometheus exporter that monitors Apache NiFi cluster status and node connectivity. Built in Python, it exposes NiFi cluster metrics for monitoring and alerting.

![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=Prometheus&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-1B5C94?style=for-the-badge&logo=apache&logoColor=white)

## Features

-  **Real-time cluster monitoring**: Track connected vs total nodes
-  **Individual node status**: Monitor each node's connection state
-  **Disconnected state handling**: Accurate metrics even when cluster is down
-  **Prometheus-ready**: Standard metrics format at `/metrics`
-  **Health endpoints**: Built-in `/health` and info page
-  **Easy configuration**: Simple `.env` file setup
-  **Auto-reconnect**: Handles token expiration and connection issues

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/dubuntu13/nifi-cluster-exporter.git
cd nifi-cluster-exporter

# Install dependencies
pip install -r requirements.txt

#Freedom_For_Iran