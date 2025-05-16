from local_pipeline import run_local_pipeline

#cloud functions entry point
def run_pipeline(request): 
    run_local_pipeline()
    return 'Gun violence pipeline executed successfully.'

if __name__ == "__main__":
    run_pipeline(None)
