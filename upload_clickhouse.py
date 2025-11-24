import clickhouse_connect
import pyarrow.parquet as pq
from tqdm import tqdm

HOST = "b45p3y549w.ap-south-1.aws.clickhouse.cloud"
USER = "default"
PASSWORD = "YVPJUt8.YQVLeL"

client = clickhouse_connect.get_client(
    host=HOST,
    user=USER,
    password=PASSWORD,
    secure=True
)

print("Connected to ClickHouse Cloud!")

# Change this to your table
TABLE_NAME = "gdelt.country_weekly"

PARQUET_FILE = r"D:\geosence\parquet\country_weekly_with_text.parquet"

BATCH_SIZE = 100_000  # SAFE FOR CLOUD


def upload_parquet_chunked(path, table):

    print(f"\nReading Parquet file...")
    parquet = pq.ParquetFile(path)
    total_rows = parquet.metadata.num_rows
    print(f"Total rows = {total_rows:,}")

    for batch_idx in tqdm(range(0, total_rows, BATCH_SIZE)):

        batch = parquet.read_row_group(batch_idx // parquet.schema.max_row_group_length) \
                       .to_pandas()[batch_idx % parquet.schema.max_row_group_length :
                                     (batch_idx % parquet.schema.max_row_group_length) + BATCH_SIZE]

        rows = batch.values.tolist()
        client.insert(table, rows)

    print("✔ Upload completed successfully!")


upload_parquet_chunked(PARQUET_FILE, TABLE_NAME)
