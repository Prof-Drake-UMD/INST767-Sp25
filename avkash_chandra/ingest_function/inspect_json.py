import json
import os

file_path = os.path.join(os.path.dirname(__file__), '..', 'merged_output.json')

with open(file_path, "r") as f:
    data = json.load(f)

first_two_pairs = dict(list(data.items())[:2])
print(json.dumps(first_two_pairs, indent=2))
