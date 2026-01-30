# 🛡️ Security Alert Ingestion Service

## Project Overview

Demo: <a href='http://ensys.miglabs.org'>censys.miglabs.org</a>

This project is a resilient, containerized service designed to ingest, enrich, and store security alerts from a third-party API. It demonstrates robust error handling, database-driven configuration, and observability via a custom dashboard.

> **Note:** For this demo, I took the liberty to modify the data returned by the Mock API to contain `src_ip` and `dst_ip` fields. This simulates a blocked connection from an internal network to an outside resource. We then enrich this data with **Geolocation** (of the destination IP) and **User Context** (based on source). This small change creates a more realistic security scenario for the demo.

## Features

### 1. Ingestion Engine
*   **Resilient Fetching:** Periodically polls the upstream API. Implements incremental syncing using a persistent `last_event_time` bookmark, ensuring no data is lost even after restarts.
*   **Robustness:** Uses `tenacity` for exponential backoff and retry logic on temporary failures. (3 attempts)
*   **Normalization:** Converts raw upstream alerts into a standardized `Alert` model. (Usign different adapter based on the source field)
*   **Enrichment:**
    *   **GeoIP:** Assign country code to destination IP (randomly selected from a list of countries).
    *   **User Context:** Maps source IP to hypothetical user ID.

### 2. Architecture & Design
*   **Database:** PostgreSQL is used for structured storage. It serves as the **Single Source of Truth** for:
    *   Normalized Alerts (`alerts` table).
    *   Service State (`service_state` table: `last_event_time`, `last_sync_time`, `last_error`).
    *   System Configuration (`system_config` table: `sync_interval_minutes`).
*   **Data Access Layer (DAL):** All database interactions are centralized in `ingestion_service/database.py`, ensuring clean separation of concerns and testability.
*   **Configurability:** Sync intervals can be adjusted dynamically via API without restarting the service.

### 3. Observability
*   **Health API:** `GET /health` provides:
    *   Database connectivity status.
    *   Last successful sync timestamp.
    *   Last attempted sync timestamp
    *   Last event bookmark (`last_event_time`).
    *   **Error Reporting:** Exposes the last error message if the pipeline fails.
*   **Interactive Dashboard:** A Streamlit-based UI to:
    *   Monitor system health and sync status.
    *   Visualize alert metrics (Critical count, Source distribution).
    *   Search and filter alerts by Country, User, or Severity.
    *   **Chaos Engineering:** Control the Mock API's failure rate and the Ingestor's sync interval directly from the UI.

## Project Structure

*   `ingestion_service/`: The core Python service (FastAPI + APScheduler + SQLAlchemy).
*   `mock_api/`: A simulated third-party Alerts API (FastAPI) with configurable failure rates.
*   `dashboard/`: A Streamlit frontend for monitoring and control.

## HTTP API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/alerts` | List stored alerts. Supports filters: `limit`, `hours`, `country`, `user`, `severity`. |
| `POST` | `/sync` | Manually trigger an immediate sync job. |
| `GET` | `/health` | Check service status, DB connection, and sync history. |
| `GET` | `/config` | Get current sync interval. |
| `POST` | `/config` | Update sync interval (minutes). |

## getting Started

### Prerequisites
*   Docker & Docker Compose

### Running the Project

1.  **Clone and Build:**
    ```bash
    git clone https://github.com/mignatys/censys-ingestion.git
    cd censys-ingestion
    docker-compose up --build
    ```
2.  **Update port variables in .env file if required:**
    ```bash
    HOST_PORT_DASHBOARD=8501
    HOST_PORT_INGESTOR=8000
    HOST_PORT_MOCK_API=8001
    HOST_PORT_DB=5432
    ```

3.  **Access the Services:**
    *   **Dashboard:** [http://localhost:8501](http://localhost:8501) (Main UI)
    *   **Ingestion API:** [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI)
    *   **Mock API:** [http://localhost:8001/docs](http://localhost:8001/docs)

4.  **Demo Flow:**
    *   Open the **Dashboard**.
    *   Observe the "Service Health" metrics (Syncs every 30 mins by default).
    *   Use the **Chaos Engineering** sidebar to increase the "Failure Probability" to `1.0`.
    *   Click "Trigger Sync Now" or wait for the schedule.
    *   Observe the **Error** message appearing in Red on the dashboard.
    *   Reset failure rate to `0.0` and sync again to clear the error.
    *   Adjust the "Sync Interval" to `1` minute and wait for the schedule.
    *   Observe the "Service Health" metrics (Syncs every 1 min by default).
    *   Use the **Chaos Engineering** sidebar to increase the "Failure Probability" to `0.9`.
    *   Click "Trigger Sync Now" and refresh the dahsboard several times
    *   Observe the **Retrying** message appearing in Yellow on the dashboard.
    *   Reset failure rate to `0.0` and sync again to clear the error.

    *   Use data filters to search for alerts by country, user, or severity.
    *   Adjust the **Time Window** to 6 or 24 hours
    *   Observe the **Alerts** metrics on the dashboard change.
