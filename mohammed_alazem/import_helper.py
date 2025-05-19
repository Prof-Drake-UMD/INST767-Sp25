"""Helper module to import modules from the Composer src directory"""
import os
import sys
from pathlib import Path

# Add src directory to Python path
dag_file_path = Path(os.path.abspath(__file__))
# src_path becomes /home/airflow/gcs/dags/../src -> /home/airflow/gcs/src
src_path = dag_file_path.parent.parent / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.append(str(src_path))
