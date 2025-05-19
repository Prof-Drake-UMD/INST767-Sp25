import sys
import os
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from process.build_cultural_experience import build_cultural_rows

def main():
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"cultural_experience_{timestamp}.csv"
    output_path = os.path.join(output_dir, output_filename)

    book_title = "Normal People"
    author = "Sally Rooney"

    print(f"Generating cultural rows for book: {book_title} (Author: {author})")
    rows = build_cultural_rows(book_title, author=author)
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        print(f" CSV saved to {output_path}")
    else:
        print(" No data generated.")

if __name__ == "__main__":
    main()
