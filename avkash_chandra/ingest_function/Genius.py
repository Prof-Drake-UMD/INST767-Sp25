import lyricsgenius

GENIUS_API_KEY = "L5LB08DGMN3PX8yVeh097G1MHWme2xl24iolMVz7nkZ6Oay0JkRQvHOWUhNXpvWM"
genius = lyricsgenius.Genius(GENIUS_API_KEY, skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"])
genius.verbose = False
genius.timeout = 10  # Increase timeout just in case

def get_genius_data(song_title, artist_name=None):
    try:
        song = genius.search_song(song_title, artist=artist_name) if artist_name else genius.search_song(song_title)

        if song and song.lyrics:
            lines = song.lyrics.splitlines()

            # Remove lines that are empty or contain non-lyric noise
            cleaned = []
            for line in lines:
                line = line.strip()
                if not line or any(x in line.lower() for x in [
                    "translation", "contributor", "read more", "lyrics", "embed", "you might also like"
                ]):
                    continue
                if line.startswith("[") and line.endswith("]"):
                    # Optional: include or skip verse labels like [Chorus], [Verse 1]
                    continue
                cleaned.append(line)
                if len(cleaned) >= 3:
                    break

            preview = "\n".join(cleaned) if cleaned else "Lyrics preview unavailable."

            return {
                "song_title": song.title,
                "artist": song.artist,
                "lyrics_url": song.url,
                "lyrics_preview": preview
            }

    except Exception as e:
        print(f"Failed to fetch Genius data for '{song_title}': {e}")

    return {
        "song_title": song_title,
        "artist": artist_name if artist_name else "N/A",
        "lyrics_url": "N/A",
        "lyrics_preview": "Lyrics preview unavailable."
    }
