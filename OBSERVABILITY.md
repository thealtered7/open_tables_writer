# Observability

Micrometer Observation bridged to OpenTelemetry (OTLP traces and metrics). Naming helpers live in `Observability.java`.

| | |
|--|--|
| OTel `service.name` | `open-tables-writer` |
| Span / observation name | `{prefix}.{operation}` |
| Explicit counter | `{prefix}.{operation}.records` (tag `outcome`) |
| Export | OTLP traces + metrics (15s step); defaults `localhost:4318` |

`DefaultMeterObservationHandler` also records a timer named like each span. There are no custom histograms or gauges.

### Prefixes

| Prefix | Used? |
|--------|-------|
| `table_writer_kafka` | Yes |
| `type2_dimension_kafka` | Yes |
| `iceberg_table_writer` | Yes |
| `type2_dimension_transformer` | Yes |
| `debezium_payload_flattener` | Yes |
| `datapipelines_client` | Yes |
| `delta_table_writer` | Defined only — no emit sites |

### Outcome semantics

- `observeOperation*` (poll loops): `outcome` is the supplier return string (`success` / `empty` / …) or `error` on exception.
- `observeCallable*` (most other ops): `outcome` is always `success` or `error`; intermediate return strings like `null_value` / `file_not_found` are discarded for metrics.

---

## Spans

| Span name | Represents | Attributes |
|-----------|------------|------------|
| `table_writer_kafka.poll` | One Kafka consumer poll + process batch (bronze / table-writer daemon) | `record_count`, `outcome` (`success` / `empty` / `error`) |
| `table_writer_kafka.process_record` | One `FileFlushNotification` (write + publish + commit) | `topic`, `partition`, `offset`, `extract_job_id`, `extract_buffer_id`, `table`, `outcome` |
| `type2_dimension_kafka.poll` | One Kafka poll + process batch (type2 dimension daemon) | `record_count`, `outcome` (`success` / `empty` / `error`) |
| `type2_dimension_kafka.process_record` | One `TableUpdatedNotification` (transform + commit) | `topic`, `partition`, `offset`, `extract_job_id`, `extract_buffer_id`, `table`, `outcome` |
| `iceberg_table_writer.write_to_table` | Full bronze Iceberg write for one input file | `table`, `input_file`, optional `extract_job_id` / `extract_buffer_id`, `outcome` |
| `type2_dimension_transformer.transform` | Type2/type1 silver dimension transform for one table | `table`, optional `extract_job_id` / `extract_buffer_id`, `outcome` |
| `debezium_payload_flattener.load_json_lines` | Spark JSON-lines read of CDC file | `input_file`, `outcome` |
| `debezium_payload_flattener.flatten_payload` | Expand Debezium payload structs to flat columns | `outcome` |
| `debezium_payload_flattener.convert_timestamp_columns` | Parse ISO timestamp string columns | `outcome` |
| `debezium_payload_flattener.get_output_table_path` | Resolve warehouse path from table FQN | `table`, `outcome` |
| `debezium_payload_flattener.get_date_partition` | Compute year/month/day partition | `outcome` |
| `debezium_payload_flattener.add_date_partition_columns` | Add `_year` / `_month` / `_day` columns | `outcome` |
| `datapipelines_client.table_write` | HTTP POST of table-write registration to datapipelines `/table-writes` | `table`, `outcome` |

---

## Metrics

Each live observation emits an explicit counter `{span}.records` with tag `outcome`, plus an auto timer named like the span.

| Metric name | Type | Measures | Labels |
|-------------|------|----------|--------|
| `table_writer_kafka.poll.records` | Counter | Kafka poll loop iterations (bronze daemon) | `outcome` |
| `table_writer_kafka.process_record.records` | Counter | Per flush-notification processing | `outcome` |
| `type2_dimension_kafka.poll.records` | Counter | Kafka poll loop iterations (type2 daemon) | `outcome` |
| `type2_dimension_kafka.process_record.records` | Counter | Per table-updated notification processing | `outcome` |
| `iceberg_table_writer.write_to_table.records` | Counter | Bronze Iceberg write for one input file | `outcome` |
| `type2_dimension_transformer.transform.records` | Counter | Silver type2/type1 transform for one table | `outcome` |
| `debezium_payload_flattener.load_json_lines.records` | Counter | Load CDC JSON lines | `outcome` |
| `debezium_payload_flattener.flatten_payload.records` | Counter | Flatten Debezium payload | `outcome` |
| `debezium_payload_flattener.convert_timestamp_columns.records` | Counter | Timestamp column conversion | `outcome` |
| `debezium_payload_flattener.get_output_table_path.records` | Counter | Output path resolution | `outcome` |
| `debezium_payload_flattener.get_date_partition.records` | Counter | Date partition calculation | `outcome` |
| `debezium_payload_flattener.add_date_partition_columns.records` | Counter | Add date partition columns | `outcome` |
| `datapipelines_client.table_write.records` | Counter | Datapipelines `/table-writes` HTTP POST | `outcome` |

Observation low-cardinality tags (e.g. `table`, `topic`) appear on the auto timer / span; the explicit `.records` counter always tags `outcome`.
