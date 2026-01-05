import requests
import json
import boto3
import botocore
import os
import time
import threading
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
from io import BytesIO

from limits import RateLimitItemPerMinute
from limits.storage import MemoryStorage
from limits.strategies import FixedWindowRateLimiter

BUCKET = "rasgos-dev-ia-9ca0b317-e219-4941-ba8e-184005886217"
UPLOADED_IDS_PATH = "uploaded.txt"

NUM_WORKERS = 10
REQUESTS_PER_MIN = 60   # effectively 1 req/sec average

df = pd.read_parquet("specimens_herbario_2.parquet")

# ---- resume tracking ----
uploaded = set()
if os.path.exists(UPLOADED_IDS_PATH):
    with open(UPLOADED_IDS_PATH) as f:
        uploaded = set(line.strip() for line in f)

num_uploaded = len(uploaded)

s3 = boto3.client("s3")
lock = threading.Lock()

# ---- rate limiter ----
storage = MemoryStorage()
limiter = FixedWindowRateLimiter(storage)
limit = RateLimitItemPerMinute(REQUESTS_PER_MIN)


def wait_for_slot():
    """Block until allowed by the limiter."""
    while not limiter.test(limit, "uploads"):
        time.sleep(0.05)
    limiter.hit(limit, "uploads")


def mark_uploaded(image_id):
    with lock:
        with open(UPLOADED_IDS_PATH, "a") as f:
            f.write(f"{image_id}\n")
            f.flush()
            os.fsync(f.fileno())
        uploaded.add(image_id)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=lambda retry_state: isinstance(retry_state.outcome.exception(), (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
    ))
)
def fetch(url):
    return requests.get(url, stream=True, timeout=15)

def process_row(row):
    image_id = str(row.id)

    # ---- already handled — skip ----
    if image_id in uploaded:
        return

    url = row.image_resized_60

    # ---- skip invalid or missing URLs ----
    if not url or url == "#" or not urlparse(url).scheme:
        print(f"Skipping {image_id}: invalid URL -> {url}")
        mark_uploaded(image_id)
        return

    try:
        # ---- respect rate limit ----
        wait_for_slot()

        resp = requests.get(
            url, 
            headers={"User-Agent": "Mozilla/5.0"},
            stream=True, 
            timeout=15,
        )
        resp.raise_for_status()

        # ---- upload image ----
        s3.upload_fileobj(
            resp.raw,
            BUCKET,
            f"images/{image_id}.jpg",
            ExtraArgs={"ContentType": "image/jpeg"},
        )

        # ---- build metadata dict ----
        metadata = row._asdict()
        metadata.pop("image_resized_10", None)

        buf = BytesIO(json.dumps(metadata, default=str).encode("utf-8"))

        # ---- upload metadata JSON ----
        s3.upload_fileobj(
            buf,
            BUCKET,
            f"metadata/{image_id}.json",
            ExtraArgs={"ContentType": "application/json"},
        )

        # ---- success: mark uploaded ----
        mark_uploaded(image_id)

    except botocore.exceptions.NoCredentialsError:
        # This is the “no credentials” hang you saw earlier
        print(f"[{image_id}] ❌ No AWS credentials found — stopping.")
        raise

    except Exception as e:
        # Log but DO NOT mark uploaded
        print(f"[{image_id}] failed: {e}")

    finally:
        # make sure network handle closes
        try:
            resp.close()
        except Exception:
            pass



def main():
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [
            executor.submit(process_row, row)
            for row in df.itertuples(index=False)
        ]

        with tqdm(
            total=len(df),
            initial=num_uploaded,
            desc="Uploading images"
        ) as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                finally:
                    pbar.update(1)


if __name__ == "__main__":
    main()
