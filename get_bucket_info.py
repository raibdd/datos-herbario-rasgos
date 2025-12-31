import boto3
import pandas as pd
import hashlib
import requests

BUCKET = "rasgos-dev-ia-9ca0b317-e219-4941-ba8e-184005886217"
PARQUET = "specimens_herbario.parquet"
UPLOADED_IDS_PATH = "uploaded.txt"

# -------------------------------
# 1) Load source dataframe (IDs)
# -------------------------------
print("Loading parquet…")
df = pd.read_parquet(PARQUET)
source_ids = set(df["id"].astype(str))
print("Rows in parquet:", len(source_ids))

# -------------------------------
# 2) Load uploaded.txt (claimed uploaded)
# -------------------------------
print("Loading uploaded.txt…")
uploaded = set()
with open(UPLOADED_IDS_PATH) as f:
    uploaded = {line.strip() for line in f}

print("IDs recorded in uploaded.txt:", len(uploaded))

# -------------------------------
# 3) List S3 objects that actually exist
# -------------------------------
print("Listing S3 contents (this may take a minute)…")

s3 = boto3.client("s3")

existing = set()
continuation = None

while True:
    kwargs = {"Bucket": BUCKET, "Prefix": "images/"}
    if continuation:
        kwargs["ContinuationToken"] = continuation

    resp = s3.list_objects_v2(**kwargs)

    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".jpg"):
            img_id = key.split("/")[-1].replace(".jpg", "")
            existing.add(img_id)

    if resp.get("IsTruncated"):
        continuation = resp["NextContinuationToken"]
    else:
        break

print("Actual images in S3:", len(existing))

# -------------------------------
# 4) Compare all sets
# -------------------------------

# items we say uploaded but S3 doesn't have
missing_from_s3 = uploaded - existing

# items uploaded that are NOT even in our dataset (rare but possible)
extra_in_s3 = existing - source_ids

# items that are truly uploaded correctly
true_uploaded = uploaded & existing

print("\n--- SUMMARY ---")
print("Source rows:                 ", len(source_ids))
print("Marked uploaded (uploaded.txt)", len(uploaded))
print("Actually in S3               ", len(existing))
print("OK (exists & marked)        ", len(true_uploaded))
print("Missing (marked but gone)   ", len(missing_from_s3))
print("Extra files in S3           ", len(extra_in_s3))

# -------------------------------
# 5) Save retry list
# -------------------------------
if missing_from_s3:
    with open("needs_retry.txt", "w") as f:
        for mid in sorted(missing_from_s3):
            f.write(f"{mid}\n")

    print(f"\nWrote needs_retry.txt with {len(missing_from_s3)} IDs")
else:
    print("\nEverything marked uploaded exists — nothing to retry 🎉")


# rows where ID appears more than once
dups = df[df.duplicated("id", keep=False)]

print(f"Duplicate IDs: {dups['id'].nunique()}")

def hash_url(url):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return hashlib.md5(r.content).hexdigest()
    except Exception as e:
        return f"ERROR: {e}"

results = []

for specimen_id, group in dups.groupby("id"):
    hashes = group["image_url"].apply(hash_url)
    unique_hashes = set(hashes)

    results.append({
        "id": specimen_id,
        "image_count": len(group),
        "unique_image_hashes": len(unique_hashes),
        "hashes": list(unique_hashes)
    })

dups_report = pd.DataFrame(results)
print(dups_report.head(20))
