# Airbyte Prometheus Monitoring
This project provides a monitoring solution for Airbyte using a local Prometheus instance that scrapes metrics from an Airbyte API exporter written in Python. The metrics are then sent to Grafana Cloud via remote-write for visualization and analysis. The setup uses Docker Compose to manage both the Prometheus instance and the Python exporter service.

## What This Project Does

1. **Airbyte Metrics Exporter**: A Python script (`main.py`) fetches detailed metrics from the Airbyte API (e.g., job status, connection status, bytes synced) and exposes them via a Prometheus-compatible endpoint on port `8000`.
2. **Local Prometheus**: Scrapes metrics from the exporter and itself, then forwards them to Grafana Cloud’s Prometheus instance using remote-write.
3. **Docker Compose**: Orchestrates the Prometheus and `airbyte-metrics` services in a single network, simplifying deployment and management.

## Setup Instructions

Follow these steps to get the project running:

### 1. Clone the Repository
Clone this repository to your local machine:

```bash
git clone https://github.com/emnaboukhris/airbyte-prometheus-monitoring.git
cd airbyte-prometheus-monitoring
```

### 2. Prepare the secrets Directory
The project relies on sensitive credentials stored in a secrets directory. These files are not included in the repository for security reasons (they’re ignored by .gitignore). You need to create them manually:

#### 2.1 Create the secrets Directory:
```bash
mkdir secrets
```

#### 2.2 Add secrets/.env for Airbyte Credentials:
Create a file named `.env` in the secrets directory with your Airbyte API credentials:
```
AIRBYTE_CLIENT_ID=your_airbyte_client_id
AIRBYTE_CLIENT_SECRET=your_airbyte_client_secret
```
Replace `your_airbyte_client_id` and `your_airbyte_client_secret` with your actual Airbyte credentials.

#### 2.3 Add secrets/username.txt for Prometheus Username:
Create a file named `credentials.json` with your Grafana Cloud username and password:
```
{
    "username": "",
    "password": 
  }
```

### 3. Run docker-compose up
Build and start the services using Docker Compose:
```bash
docker-compose up --build -d
```

### 4. Access the Grafana Dashboard
Once the services are running, you can access the Grafana dashboard to visualize the metrics.

Go to [Grafana Cloud Airbyte Monitoring Dashboard](https://taico.grafana.net/d/eee8hgm0sv3lse/airbyte-monitoring-dashboard) to see the dashboard.

![Grafana Dashboard](images/Grafana%20Dashboard.png)
