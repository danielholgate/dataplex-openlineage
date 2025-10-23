#!/bin/bash

#
# Publishes an OpenLineage event to the Dataplex Lineage API.
#
# Usage:
#   ./publish_lineage.sh <project_id> <template_file.json> <bigquery_table_string>
#
# Example:
#   ./publish_lineage.sh my-gcp-project event.json my-gcp-project.my_dataset.my_table
#
# --- Argument Validation ---

# Check if the correct number of arguments (exactly 3) were provided
if [ "$#" -ne 3 ]; then
    echo "Error: Invalid number of arguments."
    echo "Usage: $0 <project_id> <event_file.json> <bigquery_table_ID>"
    echo "Example: $0 my-gcp-project event.json my-gcp-project.my_dataset.my_table"
    exit 1
fi

# --- Configuration ---

PROJECT_ID="$1"
TEMPLATE_FILE="$2"
BIGQUERY_TABLE="$3"

# Hardcoded location for now (seems to always work)
LOCATION='global'

# Define name for the temporary event file we will create with the specific bigquery tablename
TEMP_PAYLOAD="temp_event_payload.json"

# Check template file actually exists before proceeding
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "Error: Template file not found: $TEMPLATE_FILE"
    exit 1
fi

echo "Preparing event payload: $TEMPLATE_FILE"

# 1. Copy the template to a new temporary file
cp "$TEMPLATE_FILE" "$TEMP_PAYLOAD"

# 2. Replace bigquery tablename placeholder in temporary file
sed -i "s/\[project_id\]\.\[dataset\]\.\[table\]/$BIGQUERY_TABLE/g" "$TEMP_PAYLOAD"

echo "Sending payload to Data Lineage API for project $PROJECT_ID..."

# 3. Get auth token and send the request to the Dataplex Lineage API
curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json; charset=utf-8" \
     -d @"$TEMP_PAYLOAD" \
     "https://datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION:processOpenLineageRunEvent"

# Add a newline for cleaner terminal output after curl
echo

# 4. Clean up the temporary file
#rm "$TEMP_PAYLOAD"

echo "Done."