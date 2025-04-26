#  🎬 Selected APIs and Justification 
 For this project, I have chosen three APIs that provide complementary movie-related data:
  
 **1. TMDB API (The Movie Database)** – Provides a list of movies currently playing in theaters and real-time popularity metrics, helping track trending films.

 **2. OMDb API (Open Movie Database)** – Offers comprehensive movie information, including ratings from multiple sources (IMDB, Rotten Tomatoes, Metacritic) and box office revenue.
 
 **3. Watchmode API** – Tracks streaming availability across multiple platforms, showing where a movie can be watched online.


##  1️⃣ TMDB API – Real-Time Popularity & Metadata

📌 Why this API?
* TMDB provides a list of currently playing movies and real-time popularity scores, which update daily.
* The popularity score is a valuable metric for identifying trending movies, which OMDb does not provide.
* TMDB also provides External IDs (such as IMDb ID), which can be used to look up the movie in other APIs (like OMDb and Watchmode).

### Get Now Playing
https://api.themoviedb.org/3/movie/now_playing

**Inputs (API Request Parameters)**

| Parameter |Type| Default | Description | Example |
| ----------- | ----------- | ----------- | ----------- | ----------- |
| language | string | en-US | The language of the returned data	|	language=en-US |
| page | int32 |1|The page number for pagination| page=1 |
| region |string |ISO-3166-1| code|Filter movies by a specific country/region| region=US |


**Expected Output (Key Fields in JSON Response)**
```
{
  "dates": {
    "maximum": "2025-03-12",
    "minimum": "2025-01-29"
  },
  "page": 1,
  "results": [
    {
      "adult": false,
      "backdrop_path": "/9nhjGaFLKtddDPtPaX5EmKqsWdH.jpg",
      "genre_ids": [
        10749,
        878,
        53
      ],
      "id": 950396,
      "original_language": "en",
      "original_title": "The Gorge",
      "overview": "Two highly trained operatives grow close from a distance after being sent to guard opposite sides of a mysterious gorge. When an evil below emerges, they must work together to survive what lies within.",
      "popularity": 210.407,
      "poster_path": "/7iMBZzVZtG0oBug4TfqDb9ZxAOa.jpg",
      "release_date": "2025-02-13",
      "title": "The Gorge",
      "video": false,
      "vote_average": 7.8,
      "vote_count": 1807
    },
...
  "total_pages": 219,
  "total_results": 4365
}
```

### Get External IDs
https://api.themoviedb.org/3/movie/{movie_id}/external_ids

**Inputs (API Request Parameters)**
| Parameter | Type |  Description | Example |
| ----------- | ----------- | ----------- | ----------- |
| movie_id | int32 |  to get IMDb ID 	|950396 |


**Expected Output (Key Fields in JSON Response)**
```
{
  "id": 950396,
  "imdb_id": "tt13654226",
  "wikidata_id": "Q116971304",
  "facebook_id": null,
  "instagram_id": null,
  "twitter_id": null
}
```

##  2️⃣ OMDb API – Comprehensive Movie Details & Ratings
📌 Why this API?
* OMDb aggregates multiple rating sources (IMDB, Rotten Tomatoes, Metacritic).
* It also includes box office revenue and award information.

**Inputs (API Request Parameters)**

| Parameter | Description | Example |
| ----------- | ----------- |----------- |
| i | A valid IMDb ID | tt1285016 |


**Expected Output (Key Fields in JSON Response)**
```
{
  "Title": "The Social Network",
  "Year": "2010",
  "Rated": "PG-13",
  "Released": "01 Oct 2010",
  "Runtime": "120 min",
  "Genre": "Biography, Drama",
  "Director": "David Fincher",
  "Writer": "Aaron Sorkin, Ben Mezrich",
  "Actors": "Jesse Eisenberg, Andrew Garfield, Justin Timberlake",
  "Plot": "On a fall night in 2003, Harvard undergrad and computer programming genius Mark Zuckerberg sits down at his computer and heatedly begins working on a new idea. In a fury of blogging and programming, what begins in his dorm room soon becomes a global social network and a revolution in communication. A mere six years and 500 million friends later, Mark Zuckerberg is the youngest billionaire in history... but for this entrepreneur, success leads to both personal and legal complications.",
  "Language": "English, French",
  "Country": "United States",
  "Awards": "Won 3 Oscars. 174 wins & 188 nominations total",
  "Poster": "https://m.media-amazon.com/images/M/MV5BMjlkNTE5ZTUtNGEwNy00MGVhLThmZjMtZjU1NDE5Zjk1NDZkXkEyXkFqcGc@._V1_SX300.jpg",
  "Ratings": [
    {
      "Source": "Internet Movie Database",
      "Value": "7.8/10"
    },
    {
      "Source": "Rotten Tomatoes",
      "Value": "96%"
    },
    {
      "Source": "Metacritic",
      "Value": "95/100"
    }
  ],
  "Metascore": "95",
  "imdbRating": "7.8",
  "imdbVotes": "779,958",
  "imdbID": "tt1285016",
  "Type": "movie",
  "DVD": "N/A",
  "BoxOffice": "$96,962,694",
  "Production": "N/A",
  "Website": "N/A",
  "Response": "True"
}

```



##  3️⃣ Watchmode API – Streaming Availability
📌 Why this API?
* Watchmode tracks over 200 streaming services in 50+ countries, allowing us to see where movies can be streamed, rented, or purchased.
* Helps analyze how theatrical vs. streaming distribution impacts movie success.

**Inputs (API Request Parameters)**

/v1/title/{title_id}/sources/


| Parameter | Description | Example |
| ----------- | ----------- |----------- |
| title_id | Movie ID (IMDB, TMDB, or Watchmode ID) | title_id=345534 |
| regions (optional) | Pass one of the 2 character country codes from the /regions/ endpoint to filter the streaming source results to certain countries. By default the API will return all regions. Pass multiple countries by submitting them comma separated. |

**Expected Output (Key Fields in JSON Response)**
```
[
  {
    "source_id": 349,
    "name": "iTunes",
    "type": "buy",
    "region": "GB",
    "ios_url": "https://tv.apple.com/gb/episode/winter-is-coming/umc.cmc.11q7jp45c84lp6d16zdhum6ul?playableId=tvs.sbd.9001%3A477721657&amp;showId=umc.cmc.7htjb4sh74ynzxavta5boxuzq",
    "android_url": null,
    "web_url": "https://tv.apple.com/gb/episode/winter-is-coming/umc.cmc.11q7jp45c84lp6d16zdhum6ul?playableId=tvs.sbd.9001%3A477721657&amp;showId=umc.cmc.7htjb4sh74ynzxavta5boxuzq",
    "format": "HD",
    "price": 2.49,
    "seasons": 8,
    "episodes": 73
  },
  {
    "source_id": 387,
    "name": "HBO MAX",
    "type": "sub",
    "region": "US",
    "ios_url": "hbomax://deeplink/eyJjb21ldElkIjoidXJuOmhibzplcGlzb2RlOkdWVTROWWd2UFFsRnZqU29KQWJtTCIsImdvVjJJZCI6InVybjpoYm86ZXBpc29kZTpHVlU0TllndlBRbEZ2alNvSkFibUwifQ==?action=open",
    "android_url": "hbomax://urn:hbo:episode:GVU4NYgvPQlFvjSoJAbmL",
    "web_url": "https://play.hbomax.com/episode/urn:hbo:episode:GVU4NYgvPQlFvjSoJAbmL",
    "format": "HD",
    "price": null,
    "seasons": 8,
    "episodes": 73
  },
  ...
]
```


## 🛠️ Updated Data Flow

1. Extract "Now Playing" movies from TMDB API, including basic metadata and TMDB movie IDs.

2. Get IMDb IDs for each movie via TMDB External IDs API.

3. Fetch movie details (ratings, box office, awards) from OMDb API using IMDb ID.

4. Search Watchmode by IMDb ID to obtain the correct Watchmode title ID.

5. Retrieve streaming availability from Watchmode API using Watchmode title ID.

6. Merge all data into a unified JSON structure, ready for cleaning and loading into Google Cloud Storage and BigQuery.
