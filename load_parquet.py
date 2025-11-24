from clickhouse_driver import Client

client = Client(
    host='localhost',
    port=9000,
    user='akshat',
    password='12345'
)

client.execute("USE gdelt")

paths = {
    "daily": "/var/lib/clickhouse/user_files/country_daily_with_text.parquet",
    "weekly": "/var/lib/clickhouse/user_files/country_weekly_with_text.parquet",
    "actor": "/var/lib/clickhouse/user_files/actor_pair_with_text.parquet"
}


print("Loading daily...")
client.execute(f"""
INSERT INTO country_daily SELECT * FROM file('{paths['daily']}', 'Parquet')
""")
print("Daily loaded.")

print("Loading weekly...")
client.execute(f"""
INSERT INTO country_weekly SELECT * FROM file('{paths['weekly']}', 'Parquet')
""")
print("Weekly loaded.")

print("Loading actor pairs...")
client.execute(f"""
INSERT INTO actor_pair SELECT * FROM file('{paths['actor']}', 'Parquet')
""")
print("Actor pairs loaded.")

print("\n ALL FILES LOADED SUCCESSFULLY!")
