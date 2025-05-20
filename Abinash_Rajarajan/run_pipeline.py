#!/usr/bin/env python3
import subprocess
import sys
import os

project_root = os.path.dirname(os.path.abspath(__file__))
os.environ["PYTHONPATH"] = project_root

SCRIPTS = [
    # 1) Extract
    "extract_events.py",
    "extract_weather.py",
    "extract_business.py",

    # 2) Transform
    "transformed/transform_events.py",
    "transformed/transform_weather.py",
    "transformed/transform_business.py",

    # 3) Load
    "load_to_bigquery.py",
    "load_to_bigquery_weather.py",
    "load_to_bigquery_business.py",
]

def run(script_path: str):
    print(f"\nRunning {script_path} …")
    cmd = [sys.executable, os.path.join(project_root, script_path)]
    res = subprocess.run(cmd, env=os.environ)
    if res.returncode != 0:
        print(f"{script_path} failed (exit {res.returncode})")
        sys.exit(res.returncode)
    print(f"{script_path} finished successfully.")

def main():
    for script in SCRIPTS:
        run(script)
    print("\n All steps done!")

if __name__ == "__main__":
    main()
