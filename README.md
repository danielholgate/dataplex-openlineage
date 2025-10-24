# Dataplex Openlineage Samples


The [event_samples](/event_samples) directory contains sample events for all external systems currently supported by the Dataplex Lineage API, to demonstrate the namespaces. Each event is associated with a Bigquery table as the single output.

A python script ([import_lineage.py](import_lines.py)) is also provided to load events into a Google Cloud project. 
The script takes one of the events files as a template, and substitutes in the current timestamp and the provided BigQuery table ID to attach to as an output for the event

import_lineage.py
Usage:
```
import_lineage.py <project_id> <template_file.json> <bigquery_table_id>
```

example:
python import_lineage.py gcp-project2 event_samples/amazon_athena.json analytics_project123.openlineage_events.new_table



