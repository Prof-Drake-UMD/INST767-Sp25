from pubsub_ingest import run_ingest

# HTTP-triggered cloud function entry point
def run_ingest_entry(request):
    print("Trigger received, running ingest pipeline...")
    run_ingest()
    return "Gun violence pipeline executed successfully."