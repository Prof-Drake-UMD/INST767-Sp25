import os
from flask import Flask, request, jsonify
from google.cloud import bigquery
from DAG.api_calls import fetch_books

app = Flask(__name__)
PROJECT_ID = os.environ.get("GCP_PROJECT", "inst767-murano-cultural-lens")
DATASET = os.environ.get("BQ_DATASET", "cultural_lens")
TABLE = "books"

@app.route("/ingest_openlibrary", methods=["POST"])
def ingest_openlibrary():
    try:
        rows = fetch_books()
        if not rows:
            return jsonify({"message": "No book data found."}), 404
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            return jsonify({"message": "BigQuery insert errors", "errors": errors}), 500
        return jsonify({"message": f"Inserted {len(rows)} book rows to BigQuery."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
