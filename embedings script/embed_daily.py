# embed_country_daily.py
import math
import time
from clickhouse_driver import Client
from sentence_transformers import SentenceTransformer
import numpy as np
from tqdm import tqdm

# ---------- CONFIG ----------
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "akshat"
CLICKHOUSE_PASS = "12345"
TABLE = "gdelt.country_daily"   # fully qualified table name
KEY_COLS = ("country", "date")  # used in WHERE to update rows
TEXT_COL = "event_summary_text"
EMBED_COL = "embedding"
BATCH_SIZE = 128   # reduce if OOM; increase for speed if you have more RAM/GPU
RETRY_COUNT = 3
# ----------------------------

def get_client():
    return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)

def detect_device_and_load_model():
    # SentenceTransformers will auto-select CUDA if torch finds it.
    # No explicit device argument for some models; but SentenceTransformer uses torch under the hood.
    print("Loading model intfloat/e5-base (this may download weights if not cached)...")
    model = SentenceTransformer("intfloat/e5-base")
    # model.max_seq_length etc can be checked if needed
    return model

def fetch_count(client):
    return client.execute(f"SELECT count() FROM {TABLE}")[0][0]

def fetch_batch(client, limit, offset):
    # SELECT key cols + text; use same ORDER to be deterministic
    q = f"""
    SELECT {KEY_COLS[0]}, {KEY_COLS[1]}, {TEXT_COL}
    FROM {TABLE}
    ORDER BY {KEY_COLS[0]}, {KEY_COLS[1]}
    LIMIT {limit} OFFSET {offset}
    """
    return client.execute(q)

def update_batch(client, update_rows):
    """
    update_rows: list of tuples (embedding_list, key1, key2)
    We'll call client.execute with parameter sets; clickhouse-driver will run the query many times (one for each params tuple).
    """
    update_q = f"ALTER TABLE {TABLE} UPDATE {EMBED_COL} = %s WHERE {KEY_COLS[0]} = %s AND {KEY_COLS[1]} = %s"
    # clickhouse-driver uses %s placeholders for parameters
    # execute(query, params) where params is a list of tuples will execute in a streaming fashion
    # but to be defensive, we chunk the updates in smaller groups
    CHUNK = 64
    for i in range(0, len(update_rows), CHUNK):
        chunk = update_rows[i:i+CHUNK]
        success = False
        for attempt in range(RETRY_COUNT):
            try:
                client.execute(update_q, chunk)
                success = True
                break
            except Exception as e:
                print(f"[WARN] Update chunk failed (attempt {attempt+1}/{RETRY_COUNT}): {e}")
                time.sleep(1 + attempt*2)
        if not success:
            raise RuntimeError("Failed to update chunk after retries")

def main():
    client = get_client()
    model = detect_device_and_load_model()

    total = fetch_count(client)
    print("Total rows to process:", total)
    offset = 0
    pbar = tqdm(total=total, desc="Embedding rows", unit="rows")

    while offset < total:
        rows = fetch_batch(client, BATCH_SIZE, offset)
        if not rows:
            break

        texts = [r[2] if r[2] is not None else "" for r in rows]

        # Prepend a small hint helps some models slightly; optional
        inputs = texts  # or: [f"Event summary: {t}" for t in texts]

        embeddings = model.encode(inputs, convert_to_numpy=True, normalize_embeddings=True, batch_size=64)
        # embeddings shape: (N, 768) float32 or float64 -> convert to float32 lists
        if embeddings.dtype != np.float32:
            embeddings = embeddings.astype(np.float32)

        update_rows = []
        for i, r in enumerate(rows):
            key1 = r[0]
            key2 = r[1]
            emb_list = embeddings[i].tolist()
            update_rows.append((emb_list, key1, key2))

        # perform updates (batched)
        update_batch(client, update_rows)

        offset += len(rows)
        pbar.update(len(rows))

    pbar.close()
    print("All embeddings for country_daily done.")

if __name__ == "__main__":
    main()
