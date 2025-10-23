import csv
import json
import os
import copy

# --- Configuration ---

CSV_FILE = 'data.csv'
OUTPUT_DIR = 'output_json_files'

# The JSON template as a Python dictionary
# Using a dictionary directly is easier than loading from a string
JSON_TEMPLATE = {
  "producer": "https://github.com/my-custom-integration/my-app/v1.0",
  "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
  "eventType": "COMPLETE",
  "eventTime": "2025-10-21T21:30:00.000Z",
  "run": {
    "runId": "d4l9-4f69-b5f7-3c7b3b0d4f3e"
  },
  "job": {
    "namespace": "custom-lineage-test",
    "name": "link_external_db_to_bq_table"
  },
  "inputs": [
    {
      "namespace": "${NAMESPACE}", # This will be replaced
      "name": "${NAME}"         # This will be replaced
    }
  ],
  "outputs": [
    {
      "namespace": "bigquery",
      "name": "[project_id].[dataset].[table]"
    }
  ]
}

# --- Main Script ---

def process_csv_to_json():
    """
    Reads the CSV file, substitutes values into the JSON template,
    and writes out a new JSON file for each row.
    """
    
    # 1. Create the output directory if it doesn't exist
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Output directory created/ensured: '{OUTPUT_DIR}'")
    except OSError as e:
        print(f"Error creating directory {OUTPUT_DIR}: {e}")
        return

    # 2. Read the CSV file
    try:
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            # Use DictReader to read rows as dictionaries
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    # Get data from the row
                    filename = row['sample_file_filename']
                    namespace_val = row['namespace']
                    name_val = row['name']

                    if name_val == "\"\"":
                        name_val = ""

                    # Skip empty/invalid rows
                    if not filename or not namespace_val:
                        print(f"Skipping incomplete row: {row}")
                        continue

                    # 3. Create a deep copy of the template for this row
                    # This is crucial so that each new file is fresh
                    # and we don't modify the original template.
                    new_json_data = copy.deepcopy(JSON_TEMPLATE)

                    # 4. Substitute the values
                    new_json_data['inputs'][0]['namespace'] = namespace_val
                    new_json_data['inputs'][0]['name'] = name_val

                    # 5. Define the output file path
                    # We add the .json extension to the filename from the CSV
                    output_filename = f"{filename}"
                    output_filepath = os.path.join(OUTPUT_DIR, output_filename)

                    # 6. Write the new JSON object to its file
                    with open(output_filepath, mode='w', encoding='utf-8') as out_f:
                        # json.dump writes the dictionary as JSON
                        # indent=2 makes the file human-readable
                        json.dump(new_json_data, out_f, indent=2)
                    
                    print(f"Successfully generated: {output_filepath}")

                except KeyError as e:
                    print(f"Missing expected column {e} in row: {row}")
                except Exception as e:
                    print(f"Error processing row {row}: {e}")

    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    print("\nProcessing complete.")

# --- Run the script ---
if __name__ == "__main__":
    process_csv_to_json()