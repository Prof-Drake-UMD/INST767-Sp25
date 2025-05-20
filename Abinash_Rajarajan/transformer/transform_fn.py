# transform_fn.py
import os
import json
import base64
import pandas as pd
import functions_framework
from google.cloud import pubsub_v1
from helper.load_from_gcs import load_json_from_gcs
from helper.store_to_gcs  import store_df_to_gcs

@functions_framework.cloud_event
def transform_fn(cloud_event):
    # 1. read env-vars
    project   = os.environ["GCP_PROJECT"]
    bucket    = os.environ["RAW_BUCKET"]
    out_topic = os.environ["TRANSFORM_TOPIC"]

    # 2. decode Pub/Sub message
    data = base64.b64decode(cloud_event.data["message"]["data"])
    raw  = json.loads(data)

    # 3. load the raw JSON from GCS
    js     = load_json_from_gcs(bucket, raw["path"])
    domain = raw["domain"]

    # 4. inline transform logic
    if domain == "events":
        events = js.get("_embedded", {}).get("events", [])
        rows   = [{
            "name":          e.get("name"),
            "postalCode":    e.get("postalCode"),
            "startDateTime": e.get("dates", {}).get("start", {}).get("dateTime"),
            "endDateTime":   e.get("dates", {}).get("end",   {}).get("dateTime")
        } for e in events]
        df = pd.DataFrame(rows)

    elif domain == "weather":
        city    = js["city"]["name"]
        country = js["city"]["country"]
        lat     = js["city"]["coord"]["lat"]
        lon     = js["city"]["coord"]["lon"]
        rows = [{
            "city":         city,
            "country":      country,
            "latitude":     lat,
            "longitude":    lon,
            "weather_main": entry["weather"][0]["main"],
            "weather_desc": entry["weather"][0]["description"],
            "temperature":  entry["main"]["temp"],
            "humidity":     entry["main"]["humidity"],
            "pressure":     entry["main"]["pressure"],
            "wind_speed":   entry["wind"]["speed"],
            "timestamp":    entry["dt_txt"]
        } for entry in js.get("list", [])]
        df = pd.DataFrame(rows)

    else:  # business
        rows = [{
            "name":         biz["name"],
            "rating":       biz["rating"],
            "review_count": biz["review_count"],
            "categories":   ",".join([c["title"] for c in biz.get("categories", [])]),
            "address":      " ".join(biz["location"].get("display_address", [])),
            "city":         biz["location"].get("city"),
            "state":        biz["location"].get("state"),
            "zip_code":     biz["location"].get("zip_code"),
            "latitude":     biz["coordinates"]["latitude"],
            "longitude":    biz["coordinates"]["longitude"],
            "price":        biz.get("price", ""),
            "is_closed":    biz.get("is_closed", False)
        } for biz in js.get("businesses", [])]
        df = pd.DataFrame(rows)

    # 5. write CSV to GCS
    store_df_to_gcs(df, bucket, "transform", domain)

    # 6. publish downstream
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, out_topic)
    msg = {"bucket": bucket, "path": f"transform/{domain}.csv", "domain": domain}
    publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))
