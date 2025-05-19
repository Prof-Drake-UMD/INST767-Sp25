"""
Utility for writing transformed data to CSV files.
"""

import os
import csv
import json
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_directory(directory):
    """Ensure output directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")


def write_to_csv(data, filename, output_dir='data/processed'):
    """
    Write data to a CSV file.

    Args:
        data (list): List of dictionaries with data
        filename (str): Output filename
        output_dir (str): Output directory

    Returns:
        str: Path to the generated CSV file
    """
    # Ensure output directory exists
    ensure_directory(output_dir)

    # Generate full file path with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(output_dir, f"{filename}_{timestamp}.csv")

    if not data:
        logger.warning(f"No data to write to {file_path}")
        return None

    try:
        # Convert to DataFrame for easier CSV writing
        df = pd.DataFrame(data)

        # Write to CSV
        df.to_csv(file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

        logger.info(f"Wrote {len(data)} records to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error writing to {file_path}: {str(e)}")
        return None


def write_errors_to_json(errors, filename, output_dir='data/errors'):
    """
    Write validation errors to a JSON file.

    Args:
        errors (list): List of error dictionaries
        filename (str): Output filename
        output_dir (str): Output directory

    Returns:
        str: Path to the generated JSON file
    """
    # Ensure output directory exists
    ensure_directory(output_dir)

    # Generate full file path with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(output_dir, f"{filename}_{timestamp}.json")

    if not errors:
        logger.info(f"No errors to write to {file_path}")
        return None

    try:
        # Prepare errors for JSON serialization
        serializable_errors = []
        for error in errors:
            # Convert record to string to ensure it's serializable
            record_str = str(error.get('record', {}))
            serializable_errors.append({
                'record': record_str,
                'error': error.get('error', '')
            })

        # Write to JSON
        with open(file_path, 'w') as f:
            json.dump(serializable_errors, f, indent=2)

        logger.info(f"Wrote {len(errors)} errors to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error writing errors to {file_path}: {str(e)}")
        return None