#!/usr/bin/env python3

"""
Publishes an OpenLineage event to the Dataplex Lineage API

Usage:
  python3 publish_lineage.py <project_id> <template_file.json> <bigquery_table_id>

Example:
  python3 publish_lineage.py my-gcp-project event.json my-gcp-project.my_dataset.my_table

Requires:
  requires 'openlineage-python' 'requests' and 'google-auth' libraries.
  Install them using pip:
    pip install requests google-auth
"""

import sys
import os.path
import json
import requests
import datetime
from openlineage.client.uuid import generate_new_uuid
import argparse
from google.auth import default
from google.auth.transport.requests import Request
from google.auth.exceptions import DefaultCredentialsError
from requests.exceptions import RequestException

def parse_arguments():
    """Parses command-line arguments."""
    
    parser = argparse.ArgumentParser(
        description="Publishes an OpenLineage event to the Dataplex Lineage API.",
        epilog="Example: python import_lineage.py my-gcp-project event.json my-gcp-project.my_dataset.my_table"
    )
    
    # Define the positional arguments
    parser.add_argument(
        "project_id",
        type=str,
        help="Google Cloud project ID."
    )
    parser.add_argument(
        "template_file",
        type=str,
        help="The path to the JSON event template file."
    )
    parser.add_argument(
        "bigquery_table_id",
        type=str,
        help="The full BigQuery table ID (e.g., 'project.dataset.table')."
    )

    # Handle the case where no arguments are given, print help
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # Parse the arguments
    args = parser.parse_args()
    
    return args

def main():
    """Main execution function."""

    args = None

     # Parse arguments
    try:
        args = parse_arguments()

        project_id = args.project_id
        template_file = args.template_file
        output_table_id = args.bigquery_table_id 

        print(f"Project ID: {project_id}")
        print(f"Template File: {template_file}")
        print(f"BigQuery Table ID: {output_table_id}")

    except SystemExit:
        # catche sys.exit(1) from parser.print_help
        # or errors from parser.parse_args()
        sys.exit(1) # Ensure the script exits with an error code
    except Exception as e:
        print(f"An error occurred during argument parsing: {e}", file=sys.stderr)
        sys.exit(1)

    # Hardcode location for now. Seems to always work for any lineage creation
    location = 'global'

    # Check template file actually exists before proceeding
    if not os.path.isfile(template_file):
        print(f"Error: Template file not found: {template_file}", file=sys.stderr)
        sys.exit(1)

    try:
        # 1. Read the template JSON file content
        with open(template_file, 'r', encoding='utf-8') as f:
            template_content = f.read()
            template_json = json.loads(template_content)

            # Set the lineage event time to current time
            template_json['eventTime'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Set output BigQuery table
            template_json['outputs'][0]['name'] = output_table_id
            # Set unique run ID (not necessary, but for completeness sake)
            uuid = generate_new_uuid();
            template_json['run']['runId'] = str(uuid)

        #print(json.dumps(template_json, indent=4))  print request
        payload_data = json.dumps(template_json)

        print(f"Sending lineage event to Lineage API for project {project_id}...")

        # 3. Get auth token using default application credentials
        # This securely finds credentials (e.g., from gcloud login, service account)
        print("Authenticating with Google Cloud default credentials...")
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            token = credentials.token
        except DefaultCredentialsError:
            print("Error: Could not find default Google Cloud credentials.", file=sys.stderr)
            print("Please run 'gcloud auth application-default login' to authenticate.", file=sys.stderr)
            sys.exit(1)

        if not token:
            print("Error: Failed to retrieve auth token. Please check your authentication.", file=sys.stderr)
            sys.exit(1)

        # 4. Send the request to the Dataplex Lineage API
        api_url = f"https://datalineage.googleapis.com/v1/projects/{project_id}/locations/{location}:processOpenLineageRunEvent"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json; charset=utf-8'
        }

        response = requests.post(api_url, headers=headers, data=payload_data)

        # Raise exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        print(json.dumps(response.json(), indent=2))

    except FileNotFoundError:
        print(f"Error: File not found during read: {template_file}", file=sys.stderr)
        sys.exit(1)
    except RequestException as e:
        # Handle errors from the requests library (network, HTTP status)
        print(f"\nError sending request to Data Lineage API: {e}", file=sys.stderr)
        if e.response is not None:
            print(f"Response status: {e.response.status_code}", file=sys.stderr)
            print(f"Response body: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Catch any other unexpected errors
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()