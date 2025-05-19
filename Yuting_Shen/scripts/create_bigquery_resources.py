"""
Create BigQuery resources from SQL files.
This script executes the SQL CREATE statements to set up the BigQuery dataset and tables.
"""

import os
import argparse
from google.cloud import bigquery


def read_sql_file(file_path):
    """Read a SQL file."""
    with open(file_path, 'r') as f:
        return f.read()


def execute_sql(client, sql):
    """Execute a SQL statement."""
    query_job = client.query(sql)
    query_job.result()  # Wait for the query to complete
    return query_job


def main():
    """Main function to create BigQuery resources."""
    parser = argparse.ArgumentParser(description='Create BigQuery resources')
    parser.add_argument('--project', help='Google Cloud project ID')
    args = parser.parse_args()

    # Use provided project ID or default to environment variable
    project_id = args.project or os.environ.get('GOOGLE_CLOUD_PROJECT')

    if not project_id:
        print("Error: No Google Cloud project ID provided")
        print("Either pass --project or set GOOGLE_CLOUD_PROJECT environment variable")
        return

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    print(f"Creating BigQuery resources in project: {project_id}")

    # Get SQL files in order (they should start with numbers)
    sql_dir = 'sql/create_tables'
    sql_files = sorted([f for f in os.listdir(sql_dir) if f.endswith('.sql')])

    for sql_file in sql_files:
        file_path = os.path.join(sql_dir, sql_file)
        print(f"Executing SQL from {file_path}...")

        sql = read_sql_file(file_path)
        try:
            execute_sql(client, sql)
            print(f"✓ Successfully executed {sql_file}")
        except Exception as e:
            print(f"✗ Error executing {sql_file}: {str(e)}")

    print("BigQuery resource creation complete")


if __name__ == "__main__":
    main()