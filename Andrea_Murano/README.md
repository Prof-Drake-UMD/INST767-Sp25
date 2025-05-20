# Contextual Lens: Literature & Culture Explorer

This repository contains a cross-media cultural recommendation tool that takes a book title and author as input and returns a set of artworks and music tracks released in the same, or close to the same, year. The project integrates data engineering, SQL analytics, and API usage.

## Project Purpose

This tool is designed for analysts, educators, and cultural explorers seeking cross-medium context. By considering contemporary artworks and music, users can gain a richer understanding of the culture surrounding a given text.

## Repository Structure

- **README.md**: This file. Project overview and instructions.
- **create_tables.sql**: SQL DDL scripts for creating tables for books, artworks, and music in BigQuery.
- **queries.sql**: Example and dynamic queries to explore and join the cultural data.
- **requirements.txt**: Python dependencies for API integration and BigQuery access.
- **DAG/**: Placeholder for workflow/orchestration scripts (e.g., Airflow DAGs).
- **cf/**: Placeholder for Cloud Functions or configuration files.
- **screenshots/**: Screenshots of the application or workflow in use.

## Data Model

The system uses three main tables:

1. **Books Table**
   - Fields: book_id, title, author_name, first_publish_year, language, book_url, ingest_ts

2. **Artwork Table**
   - Fields: object_id, title, artist_name, medium, object_date, object_url, image_url, ingest_ts

3. **Music Table**
   - Fields: track_id, title, artist, album, release_date, preview_url, ingest_ts

## API Integrations

- **Open Library API**: Fetches book metadata
- **The Met Museum Collection API**: Retrieves artworks by year
- **Spotify API**: Gets music tracks by release year

## Inputs and Filters

- Book title and author name (required)
- English-language books only (`"eng"` language code)
- Artworks and tracks are selected from within ±10 years (or configurable) of the book’s first publication year

## Outputs

A result dictionary or joined table containing:
- Book metadata
- Up to three artworks
- A playlist of matched music tracks

## Example Output

```json
{
  "book": {
    "title": "Beloved",
    "author_name": "Toni Morrison",
    "first_publish_year": 1987
  },
  "artworks": [...],
  "music": [...]
}
```

## Setup

1. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure environment variables for API keys as needed.
3. Create tables in BigQuery using `create_tables.sql`.
4. Use or adapt queries from `queries.sql` for exploration.

## Screenshots

See the `screenshots/` folder for example outputs and UI.

---
- Music with previews within N years of a book (`@book_title`, `@buffer_years`)
