import requests

api_key = '89afa8b67db8d932ae8b8ef09300d480'

def get_lastfm_data(search_text):
    url = f"http://ws.audioscrobbler.com/2.0/?method=track.search&track={search_text}&api_key={api_key}&format=json"
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()
            track = data["results"]["trackmatches"]["track"][0]
            return {
                "song_title": track["name"],
                "artist": track["artist"],
                "listeners": int(track["listeners"]),
                "url": track["url"]
            }
        except (KeyError, IndexError, ValueError) as e:
            print("Error extracting track data:", e)
    else:
        print("API Error:", response.status_code)

    # Return safe fallback if anything goes wrong
    return {
        "song_title": search_text,
        "artist": "Unknown",
        "listeners": "N/A",
        "url": "N/A"
    }




