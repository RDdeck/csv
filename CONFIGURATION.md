# Configuration for kafka_to_bigquery_etl DAG

This document outlines the necessary configuration steps required to run the `kafka_to_bigquery_etl` Airflow DAG successfully.

## 1. Airflow Variables

These variables provide dynamic configuration values to the DAG tasks.

**Required Variables:**

*   **`kafka_bootstrap_servers`**: Comma-separated list of Kafka broker addresses.
    *   *Example:* `kafka-broker-1:9092,kafka-broker-2:9092`
*   **`kafka_topic`**: The Kafka topic from which messages will be consumed.
    *   *Example:* `realtime_events`
*   **`kafka_consumer_group`**: The Kafka consumer group ID for the Airflow consumer.
    *   *Example:* `airflow_etl_consumer`
*   **`postgres_conn_id`**: The Airflow Connection ID for the PostgreSQL database used for enrichment. This **must** match the ID configured in Airflow Connections (see section 2).
    *   *Example:* `postgres_enrichment_db`
*   **`gcp_project_id`**: Your Google Cloud Project ID where the target BigQuery dataset resides.
    *   *Example:* `your-gcp-project-id`
*   **`bq_dataset`**: The target BigQuery dataset name.
    *   *Example:* `data_warehouse`
*   **`bq_table`**: The target BigQuery table name within the dataset.
    *   *Example:* `events_partitioned`

**Optional Variables (with defaults):**

*   `kafka_max_messages_per_run`: Max messages to consume per DAG run (Default: `1000`)
*   `kafka_poll_timeout_ms`: Kafka consumer poll timeout in milliseconds (Default: `60000`)

**How to Set Variables:**

1.  Navigate to the Airflow UI.
2.  Go to `Admin` -> `Variables`.
3.  Click `+ Add a new record`.
4.  Enter the variable `Key` (e.g., `kafka_bootstrap_servers`) and its corresponding `Value`.
5.  Repeat for all required variables.

## 2. Airflow Connections

### PostgreSQL Connection

A connection to the PostgreSQL database is required for the `enrich_data` task.

*   **Connection ID:** Set this to the value you defined in the `postgres_conn_id` Airflow Variable.
    *   *Example:* `postgres_enrichment_db`
*   **Connection Type:** `Postgres`

**Fields to Configure in Airflow UI (`Admin` -> `Connections` -> `+ Add a new record`):**

*   `Conn Id`: `postgres_enrichment_db` (or your chosen ID)
*   `Conn Type`: `Postgres`
*   `Host`: `your-pg-host` (e.g., the hostname or IP address of your PostgreSQL server)
*   `Schema`: `your-db-name` (the name of the database)
*   `Login`: `your-pg-username`
*   `Password`: `your-pg-password`
*   `Port`: `5432` (or your PostgreSQL port)

### Google Cloud (BigQuery) Connection

The connection to Google Cloud Platform (GCP) for BigQuery access is typically handled implicitly if the Airflow worker environment is configured with appropriate credentials (e.g., a Service Account key file path set in `GOOGLE_APPLICATION_CREDENTIALS` environment variable, or using Workload Identity on GKE).

If you need to configure it explicitly:

*   **Connection ID:** You can use the default `google_cloud_default` or create a new one.
*   **Connection Type:** `Google Cloud`

Configure the necessary fields (like `Project Id`, `Keyfile Path`, `Keyfile JSON`, or `Scopes`) based on your authentication method. The `_load_to_bigquery` task uses the `gcp_project_id` variable to instantiate the client, but the underlying authentication needs to be established.

## 3. JSON Schema (for Kafka Messages)

The DAG expects incoming Kafka messages (after JSON deserialization) to conform to the following schema, defined in `kafka_to_bigquery_etl.py`:

```python
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "user_id": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"}, # ISO 8601 format expected
        "payload": {"type": "object"}
    },
    "required": ["event_id", "user_id", "timestamp"]
}
```

## 4. BigQuery Table Schema

The DAG loads data into a BigQuery table with the following schema, defined in `kafka_to_bigquery_etl.py`:

```python
BIGQUERY_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"), # Ensure this matches BQ format or is converted
    bigquery.SchemaField("payload", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_location", "STRING", mode="NULLABLE"),
]
```

**Note:** The target BigQuery table (`{gcp_project_id}.{bq_dataset}.{bq_table}`) is configured within the DAG to use daily time partitioning based on the `timestamp` field. Ensure the `timestamp` data loaded is compatible with BigQuery's TIMESTAMP type (e.g., ISO 8601 format string).

## 5. Example PostgreSQL Table (`user_profiles`)

The `enrich_data` task queries a PostgreSQL table to fetch `user_name` and `user_location` based on the `user_id` from the Kafka message. Here is an example `CREATE TABLE` statement for a table named `user_profiles`:

```sql
CREATE TABLE user_profiles (
    user_id VARCHAR(255) PRIMARY KEY,
    user_name VARCHAR(255),
    user_location VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Example Insert:**

```sql
INSERT INTO user_profiles (user_id, user_name, user_location) VALUES
('user123', 'Alice', 'New York'),
('user456', 'Bob', 'London');
```

Ensure this table exists in the PostgreSQL database configured in the Airflow Connection and contains the relevant user data for enrichment.
