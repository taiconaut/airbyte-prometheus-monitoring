# main.py
from dotenv import load_dotenv
import os
import requests
from prometheus_client import start_http_server, Gauge
import time
from datetime import datetime
from collections import defaultdict
import isodate
from config import AIRBYTE_API_URL, PROMETHEUS_PORT, METRICS_UPDATE_INTERVAL

# Load environment variables from .env file
load_dotenv()

# Get sensitive credentials from environment variables
AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")

# Check if credentials are set
if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
    raise ValueError("AIRBYTE_CLIENT_ID or AIRBYTE_CLIENT_SECRET not set in .env file")

# Dynamically derive AIRBYTE_AUTH_URL from AIRBYTE_API_URL
AIRBYTE_AUTH_URL = f"{AIRBYTE_API_URL}/applications/token"

# Prometheus Metrics Definitions
# Jobs Metrics
monitoring_num_running_jobs = Gauge('monitoring_num_running_jobs', 'Number of running jobs')
monitoring_num_pending_jobs = Gauge('monitoring_num_pending_jobs', 'Number of pending jobs')
monitoring_temporal_workflow_failure = Gauge('monitoring_temporal_workflow_failure', 'Number of failed jobs')
monitoring_num_successful_syncs = Gauge('monitoring_num_successful_syncs', 'Number of successful sync jobs')
monitoring_num_failed_syncs = Gauge('monitoring_num_failed_syncs', 'Number of failed sync jobs')
monitoring_last_successful_sync_timestamp = Gauge('monitoring_last_successful_sync_timestamp',
                                                  'Timestamp of last successful sync per connection',
                                                  ['connection_id', 'name'])
monitoring_total_bytes_synced = Gauge('monitoring_total_bytes_synced', 'Total bytes synced across all successful sync jobs')
monitoring_total_rows_synced = Gauge('monitoring_total_rows_synced', 'Total rows synced across all successful sync jobs')
monitoring_avg_successful_sync_duration = Gauge('monitoring_avg_successful_sync_duration',
                                                'Average duration of successful sync jobs in seconds')
monitoring_successful_syncs_per_connection = Gauge('monitoring_successful_syncs_per_connection',
                                                   'Number of successful syncs per connection',
                                                   ['connection_id', 'name'])
monitoring_failed_syncs_per_connection = Gauge('monitoring_failed_syncs_per_connection',
                                               'Number of failed syncs per connection',
                                               ['connection_id', 'name'])

# Connections Metrics
monitoring_active_connections = Gauge('monitoring_active_connections', 'Number of active connections')
monitoring_connection_status = Gauge('monitoring_connection_status',
                                     'Status of connections (1 for active, 0 for inactive)',
                                     ['connection_id', 'name', 'source_id', 'destination_id', 'schedule_type'])
monitoring_connection_info = Gauge('monitoring_connection_info',
                                   'Info about connections',
                                   ['connection_id', 'data_residency', 'non_breaking_schema_updates_behavior',
                                    'namespace_definition', 'prefix'])
monitoring_connection_streams_count = Gauge('monitoring_connection_streams_count',
                                            'Number of streams per connection',
                                            ['connection_id', 'name'])
monitoring_connection_created_at = Gauge('monitoring_connection_created_at',
                                         'Creation timestamp of the connection',
                                         ['connection_id', 'name'])

# Destinations Metrics
monitoring_num_destinations = Gauge('monitoring_num_destinations', 'Number of destinations')

# Sources Metrics
monitoring_num_sources = Gauge('monitoring_num_sources', 'Number of sources')

# Token Management
current_token = None
token_expiry = 0

# Global connection name mapping
connection_names = {}

def get_new_token():
    """
    Fetch a new access token using the Client ID and Client Secret.
    """
    global current_token, token_expiry
    try:
        response = requests.post(
            AIRBYTE_AUTH_URL,
            data={
                "client_id": AIRBYTE_CLIENT_ID,
                "client_secret": AIRBYTE_CLIENT_SECRET,
                "grant_type": "client_credentials"
            }
        )
        response.raise_for_status()
        token_data = response.json()
        current_token = token_data['access_token']
        token_expiry = time.time() + token_data['expires_in']
        print("New token fetched. Expires at:", time.ctime(token_expiry))
    except Exception as e:
        print(f"Error fetching new token: {e}")
        raise

def refresh_token_if_needed():
    """
    Refresh the token if it's expired or about to expire within 60 seconds.
    """
    global current_token, token_expiry
    if not current_token or time.time() >= token_expiry - 60:
        get_new_token()

def fetch_airbyte_data(endpoint):
    """
    Fetch data from a specific Airbyte API endpoint.
    """
    headers = {
        "Authorization": f"Bearer {current_token}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(f"{AIRBYTE_API_URL}/{endpoint}", headers=headers)
        response.raise_for_status()
        return response.json().get('data', [])
    except Exception as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return []

def parse_timestamp(iso_str):
    """
    Parse ISO 8601 timestamp to Unix timestamp.

    Args:
        iso_str (str): ISO 8601 timestamp (e.g., '2023-01-01T00:00:00Z')

    Returns:
        int: Unix timestamp in seconds
    """
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return int(dt.timestamp())

def update_job_metrics(jobs):
    """
    Update Prometheus metrics for jobs, including general and sync-specific metrics.

    Args:
        jobs (list): List of job dictionaries from the Airbyte API
    """
    running_jobs = 0
    pending_jobs = 0
    workflow_failures = 0
    successful_syncs = 0
    failed_syncs = 0
    last_successful_sync = {}  # Maps connectionId to latest successful sync timestamp
    total_bytes_synced = 0
    total_rows_synced = 0
    successful_sync_durations = []  # List to store durations in seconds
    successful_syncs_per_connection = defaultdict(int)
    failed_syncs_per_connection = defaultdict(int)

    for job in jobs:
        status = job.get('status')
        # General job metrics
        if status == 'running':
            running_jobs += 1
        elif status == 'pending':
            pending_jobs += 1
        elif status == 'failed':
            workflow_failures += 1

        # Sync-specific metrics
        if job.get('jobType') == 'sync':
            connection_id = job.get('connectionId')
            name = connection_names.get(connection_id, 'unknown')
            if status == 'succeeded':
                successful_syncs += 1
                updated_at = job.get('lastUpdatedAt')
                if connection_id and updated_at:
                    ts = parse_timestamp(updated_at)
                    if connection_id not in last_successful_sync or ts > last_successful_sync[connection_id]:
                        last_successful_sync[connection_id] = ts
                successful_syncs_per_connection[connection_id] += 1
                total_bytes_synced += job.get('bytesSynced', 0)
                total_rows_synced += job.get('rowsSynced', 0)
                duration_str = job.get('duration')
                if duration_str:
                    duration_td = isodate.parse_duration(duration_str)
                    duration_seconds = duration_td.total_seconds()
                    successful_sync_durations.append(duration_seconds)
            elif status == 'failed':
                failed_syncs += 1
                failed_syncs_per_connection[connection_id] += 1

    # Calculate average duration for successful syncs
    avg_duration = sum(successful_sync_durations) / len(successful_sync_durations) if successful_sync_durations else 0

    # Set global metrics
    monitoring_num_running_jobs.set(running_jobs)
    monitoring_num_pending_jobs.set(pending_jobs)
    monitoring_temporal_workflow_failure.set(workflow_failures)
    monitoring_num_successful_syncs.set(successful_syncs)
    monitoring_num_failed_syncs.set(failed_syncs)
    monitoring_total_bytes_synced.set(total_bytes_synced)
    monitoring_total_rows_synced.set(total_rows_synced)
    monitoring_avg_successful_sync_duration.set(avg_duration)

    # Set per-connection metrics with name label
    for connection_id, ts in last_successful_sync.items():
        name = connection_names.get(connection_id, 'unknown')
        monitoring_last_successful_sync_timestamp.labels(connection_id=connection_id, name=name).set(ts)
    for connection_id in successful_syncs_per_connection:
        name = connection_names.get(connection_id, 'unknown')
        monitoring_successful_syncs_per_connection.labels(connection_id=connection_id, name=name).set(
            successful_syncs_per_connection[connection_id])
    for connection_id in failed_syncs_per_connection:
        name = connection_names.get(connection_id, 'unknown')
        monitoring_failed_syncs_per_connection.labels(connection_id=connection_id, name=name).set(
            failed_syncs_per_connection[connection_id])

def update_connection_metrics(connections):
    """
    Update Prometheus metrics for connections.

    Args:
        connections (list): List of connection dictionaries from the Airbyte API
    """
    global connection_names
    active_connections = 0
    connection_names.clear()  # Reset the mapping each update cycle
    for connection in connections:
        # Extract fields with defaults for missing values
        status = connection.get('status')
        connection_id = connection.get('connectionId')
        name = connection.get('name', 'unknown')
        source_id = connection.get('sourceId', 'unknown')
        destination_id = connection.get('destinationId', 'unknown')
        schedule_type = connection.get('schedule', {}).get('scheduleType', 'unknown')
        data_residency = connection.get('dataResidency', 'unknown')
        non_breaking_behavior = connection.get('nonBreakingSchemaUpdatesBehavior', 'unknown')
        namespace_definition = connection.get('namespaceDefinition', 'unknown')
        prefix = connection.get('prefix', 'unknown')
        created_at = connection.get('createdAt', 0)
        streams_count = len(connection.get('configurations', {}).get('streams', []))

        # Store connection name for job metrics
        connection_names[connection_id] = name

        # Determine status value
        if status == 'active':
            active_connections += 1
            status_value = 1
        else:
            status_value = 0

        # Update connection status with additional labels
        monitoring_connection_status.labels(
            connection_id=connection_id,
            name=name,
            source_id=source_id,
            destination_id=destination_id,
            schedule_type=schedule_type
        ).set(status_value)

        # Update connection info metric
        monitoring_connection_info.labels(
            connection_id=connection_id,
            data_residency=data_residency,
            non_breaking_schema_updates_behavior=non_breaking_behavior,
            namespace_definition=namespace_definition,
            prefix=prefix
        ).set(1)  # Info metrics are set to 1

        # Update streams count
        monitoring_connection_streams_count.labels(connection_id=connection_id, name=name).set(streams_count)

        # Update creation timestamp
        monitoring_connection_created_at.labels(connection_id=connection_id, name=name).set(created_at)

    # Update total active connections
    monitoring_active_connections.set(active_connections)

def update_destination_metrics(destinations):
    """
    Update Prometheus metrics for destinations.

    Args:
        destinations (list): List of destination dictionaries from the Airbyte API
    """
    num_destinations = len(destinations)
    monitoring_num_destinations.set(num_destinations)

def update_source_metrics(sources):
    """
    Update Prometheus metrics for sources.

    Args:
        sources (list): List of source dictionaries from the Airbyte API
    """
    num_sources = len(sources)
    monitoring_num_sources.set(num_sources)

def fetch_airbyte_metrics():
    """
    Fetch metrics from Airbyte API and update Prometheus metrics.
    """
    try:
        refresh_token_if_needed()

        # Fetch data from all endpoints
        jobs = fetch_airbyte_data('jobs')
        connections = fetch_airbyte_data('connections')
        destinations = fetch_airbyte_data('destinations')
        sources = fetch_airbyte_data('sources')

        # Update all metrics
        update_connection_metrics(connections)  # Update connections first to populate connection_names
        update_job_metrics(jobs)  # Then update jobs to use connection_names
        update_destination_metrics(destinations)
        update_source_metrics(sources)

    except Exception as e:
        print(f"Error fetching metrics from Airbyte: {e}")

def main():
    """
    Start the Prometheus server and continuously fetch metrics.
    """
    start_http_server(PROMETHEUS_PORT)
    print(f"Prometheus metrics server started on port {PROMETHEUS_PORT}.")
    
    while True:
        fetch_airbyte_metrics()
        time.sleep(METRICS_UPDATE_INTERVAL)

if __name__ == '__main__':
    main()