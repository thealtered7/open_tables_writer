1. DONE Propegate Job_id, extract_type (snapshot/incremental), and buffer_id through raw, bronze, and silver layers. 
    - propegate through datapipelines metadata as well
2. DONE Use job_id and extract_type to handle silver layer builds
3. Detect deletes on snapshot to snapshot runs.
4. Handle schema changes
5. Better catalog infrastructure.
6. DONE Schema registry (Confluent HTTP for notification topics; Glue provider stubbed)