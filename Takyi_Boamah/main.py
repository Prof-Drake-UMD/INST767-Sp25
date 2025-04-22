from transform_to_csv import *
import subprocess

def run_pipeline():
    transform_carbon()
    transform_weather()
    transform_eia()
    subprocess.run(["python", "load_to_bigquery.py"])

if __name__ == "__main__":
    run_pipeline()
