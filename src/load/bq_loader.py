from google.cloud import bigquery

client = bigquery.Client()

uri = "gs://glamira-data-lake/bronze/*.jsonl"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
)

load_job = client.load_table_from_uri(
    uri,
    "glamira-data-2026-project.glamira_raw.products",
    job_config=job_config,
)

load_job.result()

print("Loaded to BigQuery")