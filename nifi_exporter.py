#!/usr/bin/env python3
"""
NiFi Cluster Exporter for Prometheus
Monitors NiFi cluster status and exports metrics
Loads configuration from .env file or environment variables
"""

import sys
import os
import time
import json
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# For making HTTP calls to NiFi
import requests

# For Prometheus metrics
from prometheus_client import Gauge, generate_latest, REGISTRY

# Turn off SSL warnings since we're using self-signed certs
import urllib3
urllib3.disable_warnings()

# ------------------------------------------------------------
# Configuration - Load from .env file or environment variables
# ------------------------------------------------------------

def load_config():
    """
    Load configuration from .env file or environment variables
    Returns a dictionary with all config values
    """
    config = {}
    
    # Try to load from .env file first
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"[Config] Loading from {env_file}")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse KEY=VALUE
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                        
                        # Set in environment (for compatibility)
                        os.environ[key] = value
        except Exception as e:
            print(f"[Config] Warning: Could not read .env file: {e}")
    
    # Required settings - will exit if not found
    required = ['NIFI_URL', 'NIFI_PASS']
    for key in required:
        value = os.getenv(key)
        if not value:
            print(f"[Config] ERROR: {key} must be set in .env file or environment")
            sys.exit(1)
        config[key] = value
    
    # Optional settings with defaults
    config['NIFI_USER'] = os.getenv('NIFI_USER', 'admin')
    config['CHECK_INTERVAL'] = int(os.getenv('CHECK_INTERVAL', '15'))
    config['EXPORTER_PORT'] = int(os.getenv('EXPORTER_PORT', '9103'))
    config['EXPORTER_HOST'] = os.getenv('EXPORTER_HOST', '0.0.0.0')
    config['REQUEST_TIMEOUT'] = int(os.getenv('REQUEST_TIMEOUT', '10'))
    
    # Parse boolean for SSL verification
    ssl_str = os.getenv('VERIFY_SSL', 'false').lower()
    config['VERIFY_SSL'] = ssl_str in ['true', 'yes', '1', 't', 'y']
    
    # Parse known nodes (comma-separated list)
    nodes_str = os.getenv('KNOWN_NODES', 'nifi01,nifi02,nifi03')
    config['KNOWN_NODES'] = [node.strip() for node in nodes_str.split(',') if node.strip()]
    
    return config

# Load configuration
CONFIG = load_config()

# Make config available as global variables for easier reading
NIFI_URL = CONFIG['NIFI_URL']
NIFI_USER = CONFIG['NIFI_USER']
NIFI_PASS = CONFIG['NIFI_PASS']
CHECK_INTERVAL = CONFIG['CHECK_INTERVAL']
EXPORTER_PORT = CONFIG['EXPORTER_PORT']
EXPORTER_HOST = CONFIG['EXPORTER_HOST']
KNOWN_NODES = CONFIG['KNOWN_NODES']
VERIFY_SSL = CONFIG['VERIFY_SSL']
REQUEST_TIMEOUT = CONFIG['REQUEST_TIMEOUT']

# ------------------------------------------------------------
# Prometheus metrics we'll expose
# ------------------------------------------------------------

# How many nodes total in the cluster
metric_nodes_total = Gauge('nifi_cluster_nodes_total', 'Total nodes in cluster')

# How many are connected right now
metric_nodes_connected = Gauge('nifi_cluster_nodes_connected', 'Connected nodes in cluster')

# Status of each node (1=up, 0=down)
metric_node_status = Gauge('nifi_node_status', 'Node status (1=connected, 0=disconnected)', ['node'])

# Can we talk to the cluster API? (1=yes, 0=no)
metric_cluster_api_up = Gauge('nifi_cluster_api_up', 'Cluster API reachable (1=yes, 0=no)')

# Last time we got good data
metric_last_good_scrape = Gauge('nifi_exporter_last_good_scrape', 'Timestamp of last successful check')

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def get_auth_token():
    """
    Get a login token from NiFi
    Returns the token string or None if failed
    """
    try:
        # NiFi login endpoint
        resp = requests.post(
            f"{NIFI_URL}/nifi-api/access/token",
            data={'username': NIFI_USER, 'password': NIFI_PASS},
            verify=VERIFY_SSL,  # Use config setting
            timeout=REQUEST_TIMEOUT
        )

        if resp.status_code == 201:  # NiFi returns 201 for token creation
            token = resp.text.strip()
            print(f"[Auth] Got token ({len(token)} chars)")
            return token
        else:
            print(f"[Auth] Failed: HTTP {resp.status_code}")
            return None

    except Exception as err:
        print(f"[Auth] Error: {err}")
        return None


def get_cluster_info(auth_token):
    """
    Get cluster status from NiFi API
    Returns the JSON data or None if failed
    """
    if not auth_token:
        print("[Cluster] No auth token")
        return None

    try:
        headers = {'Authorization': f'Bearer {auth_token}'}

        # This endpoint gives us cluster status
        resp = requests.get(
            f"{NIFI_URL}/nifi-api/controller/cluster",
            headers=headers,
            verify=VERIFY_SSL,
            timeout=REQUEST_TIMEOUT
        )

        print(f"[Cluster] HTTP {resp.status_code}")

        # Good response
        if resp.status_code == 200:
            return resp.json()

        # Special case: 404 means this node isn't in a cluster
        elif resp.status_code == 404:
            print("[Cluster] Node not in a cluster (404)")
            return None

        # Auth problem
        elif resp.status_code == 401:
            print("[Cluster] Auth failed (401)")
            return None

        # Other error
        else:
            print(f"[Cluster] Error {resp.status_code}: {resp.text[:100]}")
            return None

    except requests.exceptions.Timeout:
        print(f"[Cluster] Timeout after {REQUEST_TIMEOUT}s")
        return None
    except requests.exceptions.ConnectionError:
        print("[Cluster] Connection error")
        return None
    except Exception as err:
        print(f"[Cluster] Unexpected error: {err}")
        return None


def update_cluster_metrics(cluster_data):
    """
    Update Prometheus metrics with fresh cluster data
    """
    try:
        # Get nodes from response
        nodes = cluster_data.get('cluster', {}).get('nodes', [])

        # Count connected nodes
        connected_count = 0
        for node in nodes:
            if node.get('status') == 'CONNECTED':
                connected_count += 1

        total_count = len(nodes)

        # Update the metrics
        metric_nodes_total.set(total_count)
        metric_nodes_connected.set(connected_count)
        metric_cluster_api_up.set(1)  # API is working

        # Clear old node statuses first (important!)
        metric_node_status.clear()

        # Set status for each node we found
        for node in nodes:
            addr = node.get('address', 'unknown')
            # Try to get just the hostname
            if ':' in addr:
                hostname = addr.split(':')[0]
            else:
                hostname = addr

            # 1 for connected, 0 for disconnected
            status_val = 1 if node.get('status') == 'CONNECTED' else 0
            metric_node_status.labels(node=hostname).set(status_val)

        # Remember when we last got good data
        metric_last_good_scrape.set(time.time())

        print(f"[Metrics] Updated: {connected_count}/{total_count} nodes connected")

    except Exception as err:
        print(f"[Metrics] Error updating: {err}")
        # If something goes wrong, mark as disconnected
        mark_cluster_disconnected()


def mark_cluster_disconnected():
    """
    Set metrics to show cluster is disconnected
    Called when we can't reach the cluster
    """
    print("[Metrics] Setting disconnected state")

    # API is not reachable
    metric_cluster_api_up.set(0)

    # No nodes are connected
    metric_nodes_connected.set(0)

    # Keep total at last known value, or use our known nodes count
    # (This keeps the metric from disappearing completely)
    current_total = metric_nodes_total._value.get()
    if current_total == 0:
        metric_nodes_total.set(len(KNOWN_NODES))

    # Clear any old node statuses
    metric_node_status.clear()

    # Mark all known nodes as disconnected (status=0)
    for node_name in KNOWN_NODES:
        metric_node_status.labels(node=node_name).set(0)


def check_cluster_loop():
    """
    Main loop that checks cluster status every few seconds
    Runs in a background thread
    """
    # Start with disconnected state
    mark_cluster_disconnected()

    while True:
        try:
            print("\n" + "="*50)
            print(f"[Check] Starting at {datetime.now().strftime('%H:%M:%S')}")

            # Step 1: Get auth token
            token = get_auth_token()

            # Step 2: Get cluster data
            cluster_data = None
            if token:
                cluster_data = get_cluster_info(token)

            # Step 3: Update metrics
            if cluster_data:
                update_cluster_metrics(cluster_data)
                print("[Check] Success")
            else:
                mark_cluster_disconnected()
                print("[Check] Failed or disconnected")

        except Exception as err:
            print(f"[Check] Loop error: {err}")
            mark_cluster_disconnected()

        # Wait before next check
        print(f"[Check] Sleeping {CHECK_INTERVAL}s...")
        time.sleep(CHECK_INTERVAL)


# ------------------------------------------------------------
# HTTP server for Prometheus
# ------------------------------------------------------------

class MetricsHandler(BaseHTTPRequestHandler):
    """
    Serves Prometheus metrics at /metrics
    """

    def do_GET(self):
        # Serve metrics
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()

            try:
                # Generate Prometheus format
                metrics_output = generate_latest(REGISTRY)
                self.wfile.write(metrics_output)
            except:
                # If metrics fail, at least return something
                self.wfile.write(b"# Error generating metrics\n")

        # Simple health check
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()

            health = {
                'status': 'running',
                'time': datetime.now().isoformat(),
                'config': {
                    'nifi_url': NIFI_URL,
                    'check_interval': CHECK_INTERVAL,
                    'known_nodes': KNOWN_NODES
                }
            }
            self.wfile.write(json.dumps(health).encode())

        # Basic info page
        elif self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()

            html = f"""
            <html>
            <head><title>NiFi Exporter</title></head>
            <body>
                <h1>NiFi Cluster Exporter</h1>
                <p>Monitoring: {NIFI_URL}</p>
                <p>Check interval: {CHECK_INTERVAL} seconds</p>
                <p>Known nodes: {', '.join(KNOWN_NODES)}</p>
                <p>SSL Verify: {VERIFY_SSL}</p>
                <hr>
                <p>Metrics at: <a href="/metrics">/metrics</a></p>
                <p>Health at: <a href="/health">/health</a></p>
            </body>
            </html>
            """
            self.wfile.write(html.encode())

        # 404 for anything else
        else:
            self.send_response(404)
            self.end_headers()

    # Don't log every request (it's noisy)
    def log_message(self, format, *args):
        pass


def start_http_server():
    """
    Start the web server that serves metrics
    """
    server = HTTPServer((EXPORTER_HOST, EXPORTER_PORT), MetricsHandler)
    print(f"[Server] Listening on {EXPORTER_HOST}:{EXPORTER_PORT}")
    print(f"[Server] Metrics at http://{EXPORTER_HOST}:{EXPORTER_PORT}/metrics")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down...")
    except Exception as err:
        print(f"[Server] Error: {err}")


# ------------------------------------------------------------
# Main program
# ------------------------------------------------------------

def main():
    """
    Main function - starts everything
    """
    print("\n" + "="*60)
    print("NiFi Cluster Exporter (with .env config)")
    print("="*60)
    print(f"NiFi URL:      {NIFI_URL}")
    print(f"Username:      {NIFI_USER}")
    print(f"Check every:   {CHECK_INTERVAL} seconds")
    print(f"Server:        {EXPORTER_HOST}:{EXPORTER_PORT}")
    print(f"Known nodes:   {', '.join(KNOWN_NODES)}")
    print(f"SSL Verify:    {VERIFY_SSL}")
    print(f"Timeout:       {REQUEST_TIMEOUT}s")
    print("="*60)
    print("Configuration loaded from .env file")
    print("Ctrl+C to stop\n")

    # Start the cluster checker in background
    checker_thread = threading.Thread(target=check_cluster_loop, daemon=True)
    checker_thread.start()

    # Start the web server (this blocks forever)
    start_http_server()


if __name__ == "__main__":
    main()
