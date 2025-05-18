# Contextual Lens: Literature & Culture Explorer

This project is an cultural recommendation tool that takes a book title and author as input and returns a set of artworks and music tracks released the same, or close to the same, year. It is designed as an immersive discovery engine for artistic content from 1950 to the present.

## Project Purpose

This tool is intended for analysts, educators, and cultural explorers seeking cross-medium context. One can gain a much greater understanding of literature by considering the context in which it was released.

## APIs Used

### 1. Open Library API
- **Input:** Book title and author name
- **Output:** Metadata including title, author, first publication year, language, and Open Library URL

### 2. The Met Museum Collection API
- **Input:** Target year (+/- buffer range)
- **Output:** Artworks created within the specified year range, including title, artist, date, medium, and image URL

### 3. Spotify API
- **Input:** Target year
- **Output:** Music tracks released in that year, including title, artist, album, release date, and preview URL (when available)

## Inputs and Filters

- Book title and author name (required)
- English-language books only (`"eng"` language code)
- Artworks and tracks are selected from within ±10 years of the book’s first publication year

## Outputs

- A dictionary containing:
  - Book metadata
  - Up to three pieces of artwork
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
