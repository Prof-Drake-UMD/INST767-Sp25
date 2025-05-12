import csv
from api_calls import get_exercises_wger

def transform_exercises():
    exercises = get_exercises_wger()

    with open("../output/exercises.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "description", "category", "equipment", "primary_muscles"])

        for ex in exercises:
            name = next((t['name'] for t in ex['translations'] if t['language'] == 2), ex['category']['name'])
            description = next((t['description'] for t in ex['translations'] if t['language'] == 2), "")
            equipment = ', '.join([eq['name'] for eq in ex['equipment']])
            muscles = ', '.join([m['name'] for m in ex['muscles']])
            writer.writerow([
                ex['id'],
                name,
                description,
                ex['category']['name'],
                equipment,
                muscles
            ])

if __name__ == "__main__":
    transform_exercises()
