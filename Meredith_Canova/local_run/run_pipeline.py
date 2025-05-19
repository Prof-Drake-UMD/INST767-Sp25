import logging
from local_run.ingest_all_local import main as ingest_main
from local_run.transform import main as transform_main

# ── Configure logging ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Starting Local Data Pipeline...")

    try:
        logging.info("Running ingestion...")
        ingest_main()
        logging.info("Ingestion complete.\n")

        logging.info("Running transformation...")
        transform_main()
        logging.info("Transformation complete.\n")

        logging.info("All steps finished. Cleaned CSVs are in local_run/outputs/")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
