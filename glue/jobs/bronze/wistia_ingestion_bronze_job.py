iimport sys
import json
import boto3
import requests
import datetime
from io import BytesIO
from awsglue.utils import getResolvedOptions

# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
args = getResolvedOptions(sys.argv, ['WISTIA_API_TOKEN', 'MEDIA_IDS', 'BRONZE_PATH'])

API_TOKEN = args['WISTIA_API_TOKEN']
MEDIA_IDS = args['MEDIA_IDS'].split(',')    # comma-separated string → list
BRONZE_PATH = args['BRONZE_PATH']
AWS_REGION = "us-east-1"

# Create the S3 client
s3 = boto3.client("s3", region_name=AWS_REGION)

# Extract bucket + prefix from full S3 path
# BRONZE_PATH: s3://rlk-wistia-video-analytics-dev/bronze/
if BRONZE_PATH.startswith("s3://"):
    parts = BRONZE_PATH.replace("s3://", "").split("/", 1)
    BRONZE_BUCKET = parts[0]
    BRONZE_PREFIX = parts[1].rstrip("/") if len(parts) > 1 else ""
else:
    raise ValueError("❌ BRONZE_PATH must start with s3://")

# Get today's date for the incremental storage
today_str = datetime.datetime.utcnow().strftime("%Y%m%d")

# Auth header for wistia
headers = {"Authorization": f"Bearer {API_TOKEN}"}

# --------------------------------------------
# HELPER: Upload JSON object to S3
# --------------------------------------------
def upload_to_S3 (data, prefix, filename):
    key = f"{prefix}/{filename}"
    body = BytesIO(json.dumps(data, indent=2).encode('utf-8'))
    s3.upload_fileobj(body, BRONZE_BUCKET, key)
    print(f"✅ Uploaded {key} ({len(data)} records)")

# --------------------------------------------
# HELPER: Paginated fetch from Wistia API
# NOTE: After an initial run, it became evident that the content is
#       not setup for pagination even though the documentation indicates
#       it is.  Therefore, make the initial fetch to get all the data at once.
# --------------------------------------------
def fetch_all_pages (url, params=None):
    if params is None:
        params = {}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    all_media = resp.json()
    print(f"✅ Retrieved {len(all_media)} media objects")
    
    return all_media
    
def filter_media(all_media):
    filtered_media = [
        {
            "id": m.get("id"),
            "hashed_id": m.get("hashed_id"),
            "name": m.get("name"),
            "duration": m.get("duration"),
            "created": m.get("created"),
            "updated": m.get("updated"),
            "thumbnail": m.get("thumbnail", {}).get("url"),
            "project": m.get("project", {}).get("name"),
        }
        for m in all_media
    ]
    
    return filtered_media

    
# --------------------------------------------
# FR3 - Fetch Media Metadata
# --------------------------------------------
def fetch_media_metadata ():
    url = "https://api.wistia.com/v1/medias.json"
    all_items = fetch_all_pages(url)
    
    # In order for the silver fact and dim tables to join,
    # they must have matching media_id's.  Ensure the two
    # media_ids in MEDIA_IDS (passed in as a param, the 
    # media ids provided in the requirements of this project)
    # are present.
    must_have = []
    for hid in MEDIA_IDS:
        r = requests.get(f"https://api.wistia.com/v1/medias/{hid}.json", headers=headers)
        r.raise_for_status()
        must_have.append(r.json())
        
    by_hid = {m.get("hashed_id"): m for m in all_items}
    for m in must_have:
        by_hid[m.get("hashed_id")] = m
        
    merged = list(by_hid.values())
    
    filtered = [
        {
            "hashed_id": m.get("hashed_id"),
            "name": m.get("name"),
            "created": m.get("created"),
            "updated": m.get("updated"),
            "duration": m.get("duration"),
            "project": (m.get("project") or {}).get("name"),
            "thumbnail": (m.get("thumbnail") or {}).get("url"),
        }
        for m in merged
    ]
    
    prefix = f"{BRONZE_PREFIX}/media_metadata"
    filename = f"metadata_stats_{today_str}.json"
    upload_to_S3(filtered, prefix, filename)
        
# --------------------------------------------
# FR4, FR6, FR7 - Fetch Engagement Metrics (Incremental)
# --------------------------------------------
def fetch_engagement_metrics():
    for media_id in MEDIA_IDS:
        url = f"https://api.wistia.com/v1/medias/{media_id}/stats.json"
        data = fetch_all_pages(url)

        # Flatten the "stats" object before writing for a more streamlined
        # Silver conversion.  This is not violating any purist principle for
        # ingested data - rather, it is taking API data (important) and 
        # applying a flattening of the structure without compromising any
        # of the data
        flattened = [{
            "hashed_id": data.get("hashed_id", media_id),
            "name": data.get("name"),
            "load_count": data.get("stats", {}).get("pageLoads"),
            "play_count": data.get("stats", {}).get("plays"),
            "play_rate": data.get("stats", {}).get("percentOfVisitorsClickingPlay"),
            "hours_watched": None,  # not provided by API
            "engagement": data.get("stats", {}).get("averagePercentWatched"),
            "visitors": data.get("stats", {}).get("visitors"),
            "created": data.get("created"),
            "updated": data.get("updated")
        }]

        prefix = f"{BRONZE_PREFIX}/{media_id}"
        filename = f"stats_{today_str}.json"
        upload_to_S3(flattened, prefix, filename)

# --------------------------------------------
# MAIN EXECUTION
# --------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting Wistia Bronze Ingestion Job")
    print(f"Bucket: {BRONZE_BUCKET}")
    print(f"Media IDs: {MEDIA_IDS}")
    print(f"Date: {today_str}")

    # 1️⃣ Fetch and store metadata
    fetch_media_metadata()

    # 2️⃣ Fetch and store engagement metrics per media ID
    fetch_engagement_metrics()

    print("🎉 Ingestion complete. Bronze layer updated.")


