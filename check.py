from clickhouse_driver import Client

# Connect to ClickHouse
client = Client(
    host='localhost',
    port=9000,
    user='akshat',
    password='12345',
    database='gdelt'
)

print("Connected to ClickHouse.")

# SQL to drop the table
drop_sql = "DROP TABLE IF EXISTS gdelt.country_weekly_emb_small"

print("Dropping table if it exists...")
client.execute(drop_sql)

print("Table dropped (or did not exist). Done!")
