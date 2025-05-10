import csv
import json
from api_calls import get_exercises

def transform_exercises():
    exercises_data = get_exercises()

    # Write exercises.csv
    with open('output/exercises.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'description', 'category', 'equipment', 'image_urls', 'last_update'])
        for ex in exercises_data['results']:
            ex_name = next((t['name'] for t in ex['translations'] if t['language'] == 2), ex['category']['name'])
            equipment_list = [eq['name'] for eq in ex['equipment']]
            images = [img['image'] for img in ex.get('images', [])]
            writer.writerow([
                ex['id'], ex_name, ex.get('description', ''), ex['category']['name'],
                json.dumps(equipment_list), json.dumps(images), ex['last_update']
            ])

    # Write muscles.csv
    unique_muscles = {}
    for ex in exercises_data['results']:
        for m in ex['muscles']:
            unique_muscles[m['id']] = m['name_en'] or m['name']
    with open('output/muscles.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['muscle_id', 'muscle_name'])
        for mid, mname in unique_muscles.items():
            writer.writerow([mid, mname])

    # Write exercise_primary_muscles.csv
    with open('output/exercise_primary_muscles.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['exercise_id', 'muscle_id'])
        for ex in exercises_data['results']:
            for m in ex['muscles']:
                writer.writerow([ex['id'], m['id']])

