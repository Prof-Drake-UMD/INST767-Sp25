!pip install yfinance
import os
import requests
import yfinance as yf

#yahoo finance 

def fetch_yahoo(ticker_symbol="AAPL", period='1mo', interval='1d'):
    ticker = yf.Ticker(ticker_symbol)
    data = ticker.history(period=period, interval=interval)
    return data



if __name__ == "__main__":
    stock_data = fetch_yahoo("AAPL")
    print("Stock Data:\n", stock_data.head())

    def fetch_yahoo_as_json(ticker_symbol="AAPL", period="1mo", interval="1d"):
    
    ticker = yf.Ticker(ticker_symbol)
    data = ticker.history(period=period, interval=interval)

    # converting dataframe to JSON string
    json_data = data.to_json(orient="records", date_format="iso")

    # converting to python object
    json_object = json.loads(json_data)

    return json_object

# saving to file 
def save_to_json_file(data, filename="finance_data.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    stock_data = fetch_yahoo_as_json("AAPL")
    save_to_json_file(stock_data, "aapl_stock_data.json")
    
#CoinGecko

def fetch_coingecko(crypto_id="bitcoin"):
    
    url = f"https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": crypto_id,
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    crypto_data = fetch_coingecko("bitcoin")
    print("Crypto Data:\n", crypto_data)
    
def fetch_fred(series_id="CPIAUCSL", api_key= "apikey"):
  
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json"
    }
    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    fred_data = fetch_fred("CPIAUCSL", api_key="3ab53daa5812f18f00c5b0873399a5bc")
    print("FRED Data:\n", fred_data["observations"][:5])