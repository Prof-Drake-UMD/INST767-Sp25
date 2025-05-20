from helper.load_from_gcs import load_json_from_gcs
from helper.store_to_gcs import store_df_to_gcs
import pandas as pd

# GCS locations
bucket = '767-abinash'
raw_prefix = 'raw'
file_name = 'yelp_businesses'           # NOTE the underscore, must match your raw JSON
transform_prefix = 'transform'

file_path = f'{raw_prefix}/{file_name}.json'

# 1) Load the raw JSON
business_json = load_json_from_gcs(bucket, file_path)

# 2) Loop through each business in the “businesses” array
business_data = []
for biz in business_json.get('businesses', []):
    info = {
        "name":         biz.get("name"),
        "rating":       biz.get("rating"),
        "review_count": biz.get("review_count"),
        "categories":   ", ".join([c.get("title","") for c in biz.get("categories", [])]),
        "address":      " ".join(biz.get("location",{}).get("display_address", [])),
        "city":         biz.get("location",{}).get("city"),
        "state":        biz.get("location",{}).get("state"),
        "zip_code":     biz.get("location",{}).get("zip_code"),
        "latitude":     biz.get("coordinates",{}).get("latitude"),
        "longitude":    biz.get("coordinates",{}).get("longitude"),
        "phone":        biz.get("display_phone"),
        "price":        biz.get("price"),
        "is_closed":    biz.get("is_closed"),
    }
    business_data.append(info)

# 3) Convert to DataFrame  
business_df = pd.DataFrame(business_data)

# 4) Write out to GCS as CSV  
store_df_to_gcs(business_df, bucket, transform_prefix, file_name)