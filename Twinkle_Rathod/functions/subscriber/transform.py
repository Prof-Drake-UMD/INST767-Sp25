#cleaning data for BigQuery

import pandas as pd
from datetime import datetime
import uuid

# DC Data transformation (ShotSpotter format)
def transform_dc_data(dc_json):
    features = dc_json.get("features", [])
    rows = []
    for feature in features:
        props = feature.get("properties", {})
        dt_str = props.get("DATETIME")

        try:
            dt_obj = pd.to_datetime(dt_str)
            incident_date = dt_obj.date()
            incident_time = dt_obj.time()
        except:
            incident_date = None
            incident_time = None

        rows.append({
            "incident_id": f"dc_{uuid.uuid4()}",
            "city": "Washington DC",
            "incident_date": incident_date,
            "incident_time": incident_time,
            "description": str(props.get("TYPE")),
            "location": str(props.get("SOURCE")),
            "latitude": float(props.get("LATITUDE")) if props.get("LATITUDE") else None,
            "longitude": float(props.get("LONGITUDE")) if props.get("LONGITUDE") else None,
            "source_dataset": "DC ArcGIS",
            "loaded_at": datetime.utcnow()
        })
    return pd.DataFrame(rows)


# NYC Data transformation
def transform_nyc_data(nyc_json):
    rows = []
    for record in nyc_json:
        try:
            parsed_time = pd.to_datetime(record.get("cmplnt_fr_tm"), format="%H:%M:%S", errors="coerce")
            incident_time = parsed_time.time() if parsed_time is not pd.NaT else None
        except:
            incident_time = None

        rows.append({
            "incident_id": f"nyc_{record.get('cmplnt_num') or uuid.uuid4()}",
            "city": "New York City",
            "incident_date": record.get("cmplnt_fr_dt"),
            "incident_time": incident_time,
            "description": str(record.get("pd_desc")),
            "location": str(record.get("boro_nm")),
            "latitude": float(record.get("latitude")) if record.get("latitude") else None,
            "longitude": float(record.get("longitude")) if record.get("longitude") else None,
            "source_dataset": "NYC Socrata",
            "loaded_at": datetime.utcnow()
        })
    return pd.DataFrame(rows)


# CDC Data transformation (State-level FA incidents)
def transform_cdc_data(cdc_json):
    rows = []
    for record in cdc_json:
        rows.append({
            "incident_id": f"cdc_{uuid.uuid4()}",
            "city": "National",
            "incident_date": f"{record['period']}-01-01",  #some would show no values since it's summarized annual statss and no time component
            "incident_time": None,
            "description": record["intent"],
            "location": record["name"],
            "latitude": None,
            "longitude": None,
            "source_dataset": "CDC State-Level",
            "loaded_at": datetime.utcnow()
        })
    return pd.DataFrame(rows)


# Combine all dataframes into one
def combine_all(dc_df, nyc_df, cdc_df):
    frames = [df for df in [dc_df, nyc_df, cdc_df] if not df.empty]
    combined = pd.concat(frames, ignore_index=True)
    combined["incident_date"] = pd.to_datetime(combined["incident_date"], errors="coerce").dt.date
    return combined
