# Openlineage Namespace examples for Dataplex / BigQuery


The [event_samples](/event_samples) directory contains sample lineage events in [Opeanlineage](https://openlineage.io/) format for all the systems which are currently supported by the Dataplex Lineage API. Each event is associated with a Bigquery table as the single output. 

See here for more details aboud custom lineage events with Dataplex [https://cloud.google.com/dataplex/docs/open-lineage](https://cloud.google.com/dataplex/docs/open-lineage)

For convenience, a python script ([import_lineage.py](import_lines.py)) is also provided to load these events into a Google Cloud project. 
The script takes one of the events files as a template, and substitutes in the current timestamp and provided BigQuery table ID to attach the event to.

Usage:
```
python import_lineage.py <project_id> <template_file.json> <bigquery_table_id>
```

Example:


install dependencies
```
pip install openlineage-python requests google-auth
```

Run
```
python import_lineage.py gcp-project2 event_samples/amazon_athena.json analytics_project123.testing_openlineage_events.bq_table
```
produces console output:
```bash
Project ID: gcp-project1122
Template File: event_samples/amazon_athena.json
BigQuery Table ID: gcp-project1122.openlineage_events_test.bq_table
Sending lineage event to Lineage API for project gcp-project1122...
Authenticating with Google Cloud default credentials...
{
  "process": "projects/9999999999/locations/global/processes/dcb385831aaea2de1ff32a5b50c4ba02",
  "run": "projects/9999999999/locations/global/processes/dcb385831aaea2de1ff32a5b50c4ba02/runs/b3619d942d00c302dd0e2339d8d5d74d",
  "lineageEvents": [
    "projects/9999999999/locations/global/processes/dcb385831aaea2de1ff32a5b50c4ba02/runs/b3619d942d00c302dd0e2339d8d5d74d/lineageEvents/74939e02-1966-472c-b4e7-c5874b59526e"
  ]
}
```

Lineage event in BigQuery:

<img width="578" height="152" alt="Screenshot 2025-10-24 13 02 07" src="https://github.com/user-attachments/assets/a93454ca-7f21-45f6-bc65-b95eaeb276ae" />




