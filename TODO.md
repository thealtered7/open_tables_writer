1. DONE Propegate Job_id, extract_type (snapshot/incremental), and buffer_id through raw, bronze, and silver layers. 
    - propegate through datapipelines metadata as well
2. DONE Use job_id and extract_type to handle silver layer builds
3. Detect deletes on snapshot to snapshot runs.
4. DONE Handle schema changes (bronze+silver evolve from Connect + DF; soft-keep deletes; coerce safe type widenings; hard-fail otherwise). Renames are treated as drop+add.
5. Better catalog infrastructure.
6. DONE Schema registry (Confluent HTTP for notification topics; Glue provider stubbed)
7. Work out semantic versioning and releasing. 
8. Shift Operating Variables from properties files to environment variables. 
9. DONE Kafka DLQ per pipeline (`kafka.dlq.topic`); commit main offset only after successful processing or successful DLQ publish.
