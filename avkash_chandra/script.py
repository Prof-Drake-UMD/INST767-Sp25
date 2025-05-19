import requests

api_key = '89afa8b67db8d932ae8b8ef09300d480'
search_text = 'love'

url = f"http://ws.audioscrobbler.com/2.0/?method=track.search&track={search_text}&api_key={api_key}&format=json"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("Error:", response.status_code)
