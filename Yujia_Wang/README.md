#  Selected APIs and Justification
 For this project, I have chosen three APIs that provide complementary movie-related data:
 
 **1. OMDb API (Open Movie Database)** – Offers comprehensive movie information, including ratings from multiple sources (IMDB, Rotten Tomatoes, Metacritic).
 
 **2. TMDB API (The Movie Database)** – Provides real-time popularity metrics and metadata for movies.
 
 **3. Watchmode API** – Tracks streaming availability across multiple platforms, showing where a movie can be watched online.


## 1️⃣ [OMDb API – Comprehensive Movie Details & Ratings] 
📌 Why this API?
* OMDb aggregates multiple rating sources (IMDB, Rotten Tomatoes, Metacritic).
* It also includes box office revenue and award information, useful for comparing theatrical performance vs. streaming popularity.

**Inputs (API Request Parameters)**
| Parameter | Description | Example |
| ----------- | ----------- |----------- |
| t | Title | t=Harry+Potter |
| apikey | Authentication key |apikey=YOUR_OMDB_API_KEY |


**Expected Output (Key Fields in JSON Response)**
```
{
  "Title": "Harry Potter and the Deathly Hallows: Part 2",
  "Year": "2011",
  "imdbRating": "8.1",
  "Ratings": [
    {"Source": "Internet Movie Database", "Value": "8.1/10"},
    {"Source": "Rotten Tomatoes", "Value": "96%"},
    {"Source": "Metacritic", "Value": "85/100"}
  ],
  "BoxOffice": "$381,447,587"
}
```


##  2️⃣ TMDB API – Real-Time Popularity & Metadata

📌 Why this API?
* TMDB provides real-time popularity scores, which update daily based on user interactions.
* The popularity score is a valuable metric for identifying trending movies, which OMDb does not provide.
* It also includes metadata such as release dates, genres, and overview descriptions.

**Inputs (API Request Parameters)**
| Parameter | Description | Example |
| ----------- | ----------- |----------- |
| query | Movie title | query=Jack+Reacher |
| api_key | Authentication key | apikey=YOUR_TMDB_API_KEY |
| language | Language of results | language=en-US |


**Expected Output (Key Fields in JSON Response)**
```
{
  "results": [
    {
      "id": 343611,
      "title": "Jack Reacher: Never Go Back",
      "release_date": "2016-10-19",
      "popularity": 26.8,
      "vote_average": 4.19,
      "vote_count": 201
    }
  ]
}
```

##  3️⃣ Watchmode API – Streaming Availability
📌 Why this API?
* Watchmode tracks over 200 streaming services in 50+ countries, allowing us to see where movies can be streamed, rented, or purchased.
* Helps analyze how theatrical vs. streaming distribution impacts movie success.

**Inputs (API Request Parameters)**

| Parameter | Description | Example |
| ----------- | ----------- |----------- |
| title_id | Movie ID (IMDB, TMDB, or Watchmode ID) | title_id=movie-278 |
| apikey | Authentication key | apiKey=YOUR_WATCHMODE_API_KEY |

**Expected Output (Key Fields in JSON Response)**
```
[
  {
    "name": "HBO MAX",
    "type": "sub",
    "region": "US",
    "web_url": "https://play.hbomax.com"
  },
  {
    "name": "iTunes",
    "type": "buy",
    "region": "GB",
    "price": 2.49
  }
]
```


## 🛠️ How These APIs Work Together## 
| API | Key Contribution  |
| ----------- | ----------- |
| OMDb | Comprehensive movie info & ratings |
| TMDB | Real-time popularity & metadata |
| Watchmode | Streaming availability |	

By integrating these three APIs, we can analyze the popularity trends of specific movie or genres, 
including their box office performance, audience ratings, and streaming availability.

