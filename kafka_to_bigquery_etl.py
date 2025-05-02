# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Kafka to BigQuery ETL DAG

This DAG is designed to consume messages from a Kafka topic, validate them against a schema,
enrich them with data from PostgreSQL, and finally load them into BigQuery.
"""

from __future__ import annotations

import pendulum

from datetime import timedelta
import logging

# Airflow imports
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Client Library Imports
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from jsonschema import validate, ValidationError
import psycopg2
from psycopg2 import OperationalError
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # Increased retries for better fault tolerance
    'retry_delay': timedelta(minutes=5),
}

# Define the expected JSON schema for events
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

# Define BigQuery Schema
BIGQUERY_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"), # Ensure this matches BQ format or is converted
    bigquery.SchemaField("payload", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_location", "STRING", mode="NULLABLE"),
]

# Define the DAG
with DAG(
    dag_id='kafka_to_bigquery_etl',
    default_args=default_args,
    description='ETL pipeline from Kafka to BigQuery with validation and enrichment',
    schedule='@hourly',  # Or use a cron expression like '0 * * * *'
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['kafka', 'bigquery', 'etl'],
) as dag:

    def _consume_from_kafka(**context):
        """
        Consumes messages from Kafka topic, deserializes them, and pushes to XCom.
        """
        # MONITORING: Record task start time
        try:
            # MONITORING: Record config fetch start time
            kafka_bootstrap_servers = Variable.get("kafka_bootstrap_servers", "localhost:9092")
            kafka_topic = Variable.get("kafka_topic", "default_topic")
            kafka_consumer_group = Variable.get("kafka_consumer_group", "airflow_etl_group")
            max_messages_per_run = int(Variable.get("kafka_max_messages_per_run", default_var=1000))
            poll_timeout_ms = int(Variable.get("kafka_poll_timeout_ms", default_var=60000)) # 60 seconds

            logging.info(f"Connecting to Kafka: {kafka_bootstrap_servers}, Topic: {kafka_topic}, Group: {kafka_consumer_group}")

            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_bootstrap_servers.split(','),
                group_id=kafka_consumer_group,
                auto_offset_reset='earliest',
                consumer_timeout_ms=poll_timeout_ms, # Stop polling after timeout if no messages
                # Use commit_sync for better reliability if needed, but increases latency
                # enable_auto_commit=False,
                value_deserializer=lambda v: v # Keep as bytes initially for error handling
            )
            # MONITORING: Record Kafka connection time/success
        except KafkaError as e:
            # MONITORING: Increment Kafka connection error counter
            logging.error(f"Kafka Consumer Initialization Error: {e}")
            raise
        except Exception as e:
             # MONITORING: Increment configuration error counter
            logging.error(f"Error retrieving Kafka configuration from Airflow Variables: {e}")
            raise

        consumed_messages = []
        message_count = 0
        try:
            logging.info("Starting Kafka message consumption loop.")
            # MONITORING: Record start of consumption loop time
            # Poll returns dict {TopicPartition: [ConsumerRecord]}
            for tp, messages in consumer.poll().items():
                 if not messages:
                     logging.info(f"No new messages received within the timeout period ({poll_timeout_ms}ms).")
                     break # Exit if poll times out

                 for message in messages:
                     if message_count >= max_messages_per_run:
                         logging.info(f"Reached max message limit ({max_messages_per_run}). Stopping consumption for this run.")
                         break # Exit inner loop

                     try:
                         # Decode and Deserialize JSON
                         message_value_str = message.value.decode('utf-8')
                         message_dict = json.loads(message_value_str)
                         consumed_messages.append(message_dict) # Store deserialized dict
                         message_count += 1
                         # MONITORING: Increment messages consumed counter
                         # logging.debug(f"Consumed message: Offset {message.offset}, Value: {message_dict}")
                     except json.JSONDecodeError as e:
                         # MONITORING: Increment message deserialization error counter
                         logging.error(f"Failed to decode JSON message at offset {message.offset}: {e}. Message value (raw): {message.value!r}")
                         # Decide how to handle bad messages (e.g., skip, send to DLQ)
                     except Exception as e:
                         # MONITORING: Increment generic message processing error counter
                         logging.error(f"Error processing message at offset {message.offset}: {e}")

                 if message_count >= max_messages_per_run:
                     break # Exit outer loop as well

            logging.info(f"Finished consuming. Total messages processed in this run: {message_count}")
             # MONITORING: Record end of consumption loop time / duration

        except KafkaError as e:
             # MONITORING: Increment Kafka consumption error counter
            logging.error(f"Kafka Error during consumption: {e}")
            # Depending on the error, might need specific handling
        except Exception as e:
             # MONITORING: Increment generic consumption error counter
            logging.error(f"An unexpected error occurred during Kafka consumption: {e}")
        finally:
            logging.info("Closing Kafka consumer.")
            consumer.close()
             # MONITORING: Record consumer close time

        # Push collected messages to XCom
        if consumed_messages:
             # MONITORING: Record number of messages pushed to XCom
            context['ti'].xcom_push(key='kafka_messages', value=consumed_messages)
            logging.info(f"Pushed {len(consumed_messages)} messages to XCom.")
        else:
            logging.info("No messages were consumed or pushed to XCom in this run.")
         # MONITORING: Record task end time / duration


     consume_kafka_task = PythonOperator(
        task_id='consume_kafka',
        python_callable=_consume_from_kafka,
    )

    def _validate_schema(**context):
        """
        Pulls messages from XCom, validates them against EVENT_SCHEMA,
        and pushes valid messages back to XCom.
        """
         # MONITORING: Record task start time
        logging.info("Starting schema validation task.")
        messages = context['ti'].xcom_pull(task_ids='consume_kafka', key='kafka_messages')

        if not messages:
            logging.info("No messages pulled from XCom for validation. Skipping.")
            context['ti'].xcom_push(key='valid_messages', value=[]) # Push empty list for downstream tasks
             # MONITORING: Record task end time / duration (early exit)
            return

        logging.info(f"Pulled {len(messages)} messages from XCom for validation.")
         # MONITORING: Record number of messages pulled from XCom
        valid_records = []
        invalid_count = 0

        for message in messages:
            try:
                # Validate the message against the schema
                 # MONITORING: Record validation start time for message
                validate(instance=message, schema=EVENT_SCHEMA)
                 # MONITORING: Record validation end time / duration for message
                valid_records.append(message)
            except ValidationError as e:
                 # MONITORING: Increment invalid message counter (schema error)
                logging.warning(f"Schema validation failed for message: {message}. Error: {e.message}")
                invalid_count += 1
                # Optionally, push invalid records to another XCom key or handle them (e.g., send to DLQ)
            except Exception as e:
                 # MONITORING: Increment invalid message counter (other error)
                logging.error(f"An unexpected error occurred during validation of message: {message}. Error: {e}")
                invalid_count += 1

        logging.info(f"Schema validation complete. Valid records: {len(valid_records)}, Invalid records: {invalid_count}")
         # MONITORING: Record final valid/invalid message counts

        # Push valid records to XCom
        if valid_records:
             # MONITORING: Record number of valid messages pushed to XCom
            context['ti'].xcom_push(key='valid_messages', value=valid_records)
            logging.info(f"Pushed {len(valid_records)} valid messages to XCom.")
        else:
            logging.info("No valid messages found after validation.")
            context['ti'].xcom_push(key='valid_messages', value=[]) # Ensure downstream tasks know there are no messages
         # MONITORING: Record task end time / duration

     validate_schema_task = PythonOperator(
         task_id='validate_schema',
        python_callable=_validate_schema,
    )

    # Define task dependencies
    consume_kafka_task >> validate_schema_task

    def _deduplicate_data(**context):
        """
        Pulls valid messages from XCom, removes duplicates based on 'event_id'
        within the current batch, and pushes unique messages back to XCom.
        """
         # MONITORING: Record task start time
        logging.info("Starting deduplication task.")
        valid_messages = context['ti'].xcom_pull(task_ids='validate_schema', key='valid_messages')

        if not valid_messages:
            logging.info("No valid messages pulled from XCom for deduplication. Skipping.")
            context['ti'].xcom_push(key='unique_messages', value=[]) # Push empty list
             # MONITORING: Record task end time / duration (early exit)
            return

        logging.info(f"Pulled {len(valid_messages)} messages from XCom for deduplication.")
         # MONITORING: Record number of messages pulled from XCom
        seen_event_ids = set()
        unique_messages = []
        duplicate_count = 0
        unique_key = 'event_id' # Key to use for deduplication

        for message in valid_messages:
            try:
                event_id = message[unique_key]
                if event_id not in seen_event_ids:
                    seen_event_ids.add(event_id)
                    unique_messages.append(message)
                else:
                     # MONITORING: Increment duplicates found counter
                    logging.info(f"Duplicate found for {unique_key}: {event_id}. Skipping message: {message}")
                    duplicate_count += 1
            except KeyError:
                 # MONITORING: Increment key error / invalid message counter
                logging.warning(f"Message missing the unique key '{unique_key}': {message}. Skipping.")
                # This should ideally not happen if schema validation is effective
                duplicate_count += 1 # Treat as invalid/duplicate for simplicity here
            except Exception as e:
                  # MONITORING: Increment generic deduplication error counter
                 logging.error(f"An unexpected error occurred during deduplication of message: {message}. Error: {e}")
                 duplicate_count += 1 # Treat as invalid/duplicate

        logging.info(f"Deduplication complete. Input records: {len(valid_messages)}, Unique records: {len(unique_messages)}, Duplicates removed: {duplicate_count}")
         # MONITORING: Record final unique/duplicate counts

        # Push unique messages to XCom
        if unique_messages:
             # MONITORING: Record number of unique messages pushed to XCom
            context['ti'].xcom_push(key='unique_messages', value=unique_messages)
            logging.info(f"Pushed {len(unique_messages)} unique messages to XCom.")
        else:
            logging.info("No unique messages found after deduplication.")
            context['ti'].xcom_push(key='unique_messages', value=[])
         # MONITORING: Record task end time / duration

     deduplicate_data_task = PythonOperator(
         task_id='deduplicate_data',
        python_callable=_deduplicate_data,
    )

    # Update task dependencies
    validate_schema_task >> deduplicate_data_task

    def _enrich_data(**context):
        """
        Pulls unique messages from XCom, enriches them with data from PostgreSQL
        based on user_id, and pushes enriched messages back to XCom.
        """
         # MONITORING: Record task start time
        logging.info("Starting data enrichment task.")
        unique_messages = context['ti'].xcom_pull(task_ids='deduplicate_data', key='unique_messages')

        if not unique_messages:
            logging.info("No unique messages pulled from XCom for enrichment. Skipping.")
            context['ti'].xcom_push(key='enriched_messages', value=[])
             # MONITORING: Record task end time / duration (early exit)
            return

        logging.info(f"Pulled {len(unique_messages)} unique messages from XCom for enrichment.")
         # MONITORING: Record number of messages pulled from XCom
        enriched_messages = []
        enrichment_failures = 0
        pg_conn_id = Variable.get("postgres_conn_id", "postgres_default") # Use Airflow Variable for Conn ID

        conn = None
        cursor = None
        try:
            # Get connection details from Airflow connection
            logging.info(f"Fetching PostgreSQL connection details for conn_id: {pg_conn_id}")
            pg_hook = BaseHook.get_hook(conn_id=pg_conn_id)
            conn_params = pg_hook.get_conn_params_dic()
            # Construct DSN (Data Source Name) for psycopg2
            dsn = f"dbname='{conn_params.get('schema')}' user='{conn_params.get('login')}' password='{conn_params.get('password')}' host='{conn_params.get('host')}' port='{conn_params.get('port', 5432)}'"
+            # MONITORING: Record DB connection start time

            logging.info(f"Connecting to PostgreSQL host: {conn_params.get('host')}")
+
            conn = psycopg2.connect(dsn)
            cursor = conn.cursor()
            logging.info("Successfully connected to PostgreSQL.")
+            # MONITORING: Record DB connection success/time

            # Prepare SQL query (Assuming a 'user_profiles' table)
            # Adjust table and column names as needed
            query = "SELECT user_name, user_location FROM user_profiles WHERE user_id = %s;"
            logging.debug(f"Prepared SQL query: {query}")

            for message in unique_messages:
                enriched_message = message.copy() # Work on a copy
                user_id = enriched_message.get('user_id')

                if user_id:
                    try:
+                        # MONITORING: Record DB query start time
                        cursor.execute(query, (user_id,))
+                        # MONITORING: Record DB query end time / duration
                        enrichment_data = cursor.fetchone() # Fetch one row

                        if enrichment_data:
                            # Assuming order: user_name, user_location
                            enriched_message['user_name'] = enrichment_data[0]
                            enriched_message['user_location'] = enrichment_data[1]
                            enriched_messages.append(enriched_message)
+                            # MONITORING: Increment enrichment success counter
                            # logging.debug(f"Successfully enriched message for user_id: {user_id}")
                        else:
                            logging.warning(f"No enrichment data found for user_id: {user_id}. Keeping original message.")
                            enriched_messages.append(enriched_message) # Keep original if no enrichment found
+                            # MONITORING: Increment enrichment failure counter (not found)
                            enrichment_failures += 1
                    except (psycopg2.Error, OperationalError) as db_err:
+                         # MONITORING: Increment enrichment failure counter (DB error)
                         logging.error(f"Database error during enrichment for user_id {user_id}: {db_err}")
                         enriched_messages.append(enriched_message) # Keep original on DB error for this user
                         enrichment_failures += 1
                    except Exception as e:
+                         # MONITORING: Increment enrichment failure counter (other error)
                         logging.error(f"Unexpected error during enrichment for user_id {user_id}: {e}")
                         enriched_messages.append(enriched_message) # Keep original on other errors
                         enrichment_failures += 1
                else:
+                    # MONITORING: Increment enrichment failure counter (missing user_id)
                    logging.warning(f"Message missing 'user_id', cannot enrich: {enriched_message}")
                    enriched_messages.append(enriched_message) # Keep message if 'user_id' is missing
                    enrichment_failures += 1

        except OperationalError as e:
+            # MONITORING: Increment DB connection error counter
            logging.error(f"PostgreSQL Connection Error: {e}")
            # If connection fails, we can't enrich anything. Push original messages.
            enriched_messages = unique_messages # Or decide on failure strategy
            enrichment_failures = len(unique_messages)
        except Exception as e:
            logging.error(f"An unexpected error occurred during enrichment setup or connection: {e}")
            enriched_messages = unique_messages
            enrichment_failures = len(unique_messages)
        finally:
            if cursor:
                cursor.close()
                logging.debug("PostgreSQL cursor closed.")
            if conn:
                conn.close()
                logging.info("PostgreSQL connection closed.")

        total_processed = len(unique_messages)
        successful_enrichments = total_processed - enrichment_failures
         # MONITORING: Record final enrichment success/failure counts
        logging.info(f"Data enrichment complete. Processed: {total_processed}, Successfully enriched: {successful_enrichments}, Failed/Missing enrichment: {enrichment_failures}")

        # Push enriched (or original if enrichment failed) messages to XCom
         # MONITORING: Record number of messages pushed to XCom
        context['ti'].xcom_push(key='enriched_messages', value=enriched_messages)
        logging.info(f"Pushed {len(enriched_messages)} messages to XCom after enrichment attempt.")
         # MONITORING: Record task end time / duration


     enrich_data_task = PythonOperator(
         task_id='enrich_data',
        python_callable=_enrich_data,
    )

    # Update task dependencies
    deduplicate_data_task >> enrich_data_task

    def _load_to_bigquery(**context):
        """
        Pulls enriched messages from XCom and loads them into a BigQuery table.
        """
         # MONITORING: Record task start time
        logging.info("Starting BigQuery load task.")
        enriched_messages = context['ti'].xcom_pull(task_ids='enrich_data', key='enriched_messages')

        if not enriched_messages:
            logging.info("No enriched messages pulled from XCom for BigQuery load. Skipping.")
             # MONITORING: Record task end time / duration (early exit)
            return

        logging.info(f"Pulled {len(enriched_messages)} enriched messages from XCom for BigQuery load.")
         # MONITORING: Record number of messages pulled from XCom

        try:
             # MONITORING: Record config fetch start time
            gcp_project_id = Variable.get("gcp_project_id") # Assumes Variable exists
            bq_dataset = Variable.get("bq_dataset", "kafka_events")
            bq_table = Variable.get("bq_table", "events")
             # MONITORING: Record config fetch end time
        except KeyError as e:
              # MONITORING: Increment configuration error counter
             logging.error(f"Required Airflow Variable not set: {e}")
             raise ValueError(f"Missing required Airflow Variable for BigQuery config: {e}")

        table_id = f"{gcp_project_id}.{bq_dataset}.{bq_table}"
        logging.info(f"Target BigQuery table: {table_id}")

        try:
             # MONITORING: Record BQ client initialization start time
            client = bigquery.Client(project=gcp_project_id)
             # MONITORING: Record BQ client initialization end time

            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                schema=BIGQUERY_SCHEMA,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, # BigQuery client handles dict conversion
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Define time partitioning on the 'timestamp' field
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp"  # Make sure the 'timestamp' field is in a format BQ understands (e.g., ISO string)
                )
                # Optional: Add clustering here if needed
                # clustering_fields=["user_id"]
            )

            logging.info(f"Starting load job to BigQuery table {table_id}...")
             # MONITORING: Record BQ load job start time
            # The client library can handle a list of dictionaries directly for JSON loads
            load_job = client.load_table_from_json(
                enriched_messages,
                table_id,
                job_config=job_config
            )

            # Wait for the load job to complete
            load_job.result()  # Waits for the job to finish

             # MONITORING: Record BQ load job end time / duration
            destination_table = client.get_table(table_id)
             # MONITORING: Increment rows loaded counter by len(enriched_messages)
            logging.info(f"Load job completed. Loaded {len(enriched_messages)} rows to {table_id}.")
             # MONITORING: Record final table row count
            logging.info(f"Table {table_id} now contains {destination_table.num_rows} rows.")

        except GoogleCloudError as e:
             # MONITORING: Increment BQ load error counter (Google Cloud Error)
            logging.error(f"BigQuery Load Error: {e}")
            # Check for specific errors if needed, e.g., load_job.errors
            if hasattr(load_job, 'errors') and load_job.errors:
                logging.error(f"Load Job Errors: {load_job.errors}")
            raise # Re-raise the exception to fail the task
        except Exception as e:
             # MONITORING: Increment BQ load error counter (Other Error)
            logging.error(f"An unexpected error occurred during BigQuery load: {e}")
            raise
         # MONITORING: Record task end time / duration

     load_to_bigquery_task = PythonOperator(
         task_id='load_to_bigquery',
        python_callable=_load_to_bigquery,
    )

    # Update task dependencies
    enrich_data_task >> load_to_bigquery_task

    logging.info("Kafka to BigQuery ETL DAG updated with BigQuery load task.")
