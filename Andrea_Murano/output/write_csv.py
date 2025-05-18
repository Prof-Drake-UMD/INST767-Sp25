from process.build_cultural_experience import build_cultural_rows
import pandas as pd

rows = build_cultural_rows("Normal People", author="Sally Rooney")

if rows:
    df = pd.DataFrame(rows)
    df.to_csv("output/cultural_experience.csv", index=False)
    print("CSV saved to output/cultural_experience.csv")
else:
    print("No data generated.")
