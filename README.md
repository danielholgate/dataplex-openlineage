# Dataplex Openlineage Samples

This repository contains sample openlineage events. Each event demonstrated attaching a different system to a Bigquery table.

load_lineage.sh loads lineage event via the [lineage API](https://cloud.google.com/dataplex/docs/reference/data-lineage/rest) and allows a specific bigquery table ID to be substitued in to load events quickly.

Usage:
```
load_lineage.sh <project_id> <template_file.json> <bigquery_table_id>
```

