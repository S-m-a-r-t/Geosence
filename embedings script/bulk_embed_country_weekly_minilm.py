"""
bulk_embed_country_weekly_minilm.py
MiniLM 384-d embedding pipeline for gdelt.country_weekly
Creates: gdelt.country_weekly_emb_small
"""

import time
from clickhouse_driver import Client
from sentence_transformers import SentenceTransformer
import numpy as np
from tqdm import tqdm

# ---------- CONFIG ----------
HOST = "localhost"
PORT = 9000
USER = "akshat"
PASS = "12345"

SRC = "gdelt.country_weekly"
DST = "gdelt.country_weekly_emb_small"

TEXT_COL = "event_summary_text"

BATCH_SIZE = 256
INSERT_CHUNK = 512
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
NORMALIZE = True
# ----------------------------

client = Client(host=HOST, port=PORT, user=USER, password=PASS)


def create_dst_table():
    client.execute(f"""             
    CREATE TABLE IF NOT EXISTS {DST}
    (
        country String,
        yearmonth String,
        week_of_month Int8,

        total_events UInt32,
        avg_quadclass Float32,
        avg_goldstein Float32,
        avg_tone Float32,

        total_mentions UInt64,
        total_articles UInt64,

        event_summary_text String,
        embedding Array(Float32)
    ) ENGINE = MergeTree()
    ORDER BY (country, yearmonth, week_of_month)
    """)
    print("Destination table ready:", DST)


def total_rows():
    return client.execute(f"SELECT count() FROM {SRC}")[0][0]


def fetch_batch(offset, limit):
    return client.execute(f"""
        SELECT country, yearmonth, week_of_month,
               total_events, avg_quadclass, avg_goldstein, avg_tone,
               total_mentions, total_articles,
               {TEXT_COL}
        FROM {SRC}
        ORDER BY country, yearmonth, week_of_month
        LIMIT {limit} OFFSET {offset}
    """)


def embed_texts(model, texts):
    embs = model.encode(
        texts,
        convert_to_numpy=True,
        normalize_embeddings=NORMALIZE,
        batch_size=128
    )
    return embs.astype(np.float32)


def chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def insert_rows(rows):
    client.execute(f"INSERT INTO {DST} VALUES", rows, types_check=True)


def main():
    print("Loading MiniLM model...")
    model = SentenceTransformer(MODEL_NAME)

    create_dst_table()

    total = total_rows()
    print("Total weekly rows:", total)

    offset = 0
    pbar = tqdm(total=total)

    t0 = time.time()

    while offset < total:
        batch = fetch_batch(offset, BATCH_SIZE)
        if not batch:
            break

        texts = [(r[-1] or "") for r in batch]
        embs = embed_texts(model, texts)

        rows_to_insert = []
        for i, row in enumerate(batch):
            rows_to_insert.append(tuple(row[:-1]) + (row[-1], embs[i].tolist()))

        for chunk in chunked(rows_to_insert, INSERT_CHUNK):
            insert_rows(chunk)

        offset += len(batch)
        pbar.update(len(batch))

    pbar.close()
    t1 = time.time()

    print(f"\nCompleted weekly MiniLM embeddings in {t1 - t0:.1f}s")
    print(f"""
⚠️ When ready to swap:
RENAME TABLE {SRC} TO {SRC}_old, {DST} TO {SRC};
DROP TABLE {SRC}_old;
    """)


if __name__ == "__main__":
    main()
