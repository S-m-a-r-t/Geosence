from clickhouse_driver import Client

client = Client(
    host='localhost',
    port=9000,
    user='akshat',
    password='12345'
)

client.execute("CREATE DATABASE IF NOT EXISTS gdelt")
client.execute("USE gdelt")

# DAILY
client.execute("""
CREATE TABLE IF NOT EXISTS country_daily (
    country String,
    date String,
    total_events UInt32,
    avg_quadclass Float32,
    avg_goldstein Float32,
    avg_tone Float32,
    total_mentions UInt64,
    total_articles UInt64,
    violent_events UInt32,
    verbal_conflict UInt32,
    cooperation_events UInt32,
    event_summary_text String
) ENGINE = MergeTree()
ORDER BY (country, date)
""")

# WEEKLY
client.execute("""
CREATE TABLE IF NOT EXISTS country_weekly (
    country String,
    yearmonth String,
    week_of_month Int8,
    total_events UInt32,
    avg_quadclass Float32,
    avg_goldstein Float32,
    avg_tone Float32,
    total_mentions UInt64,
    total_articles UInt64,
    event_summary_text String
) ENGINE = MergeTree()
ORDER BY (country, yearmonth, week_of_month)
""")

# ACTOR PAIR
client.execute("""
CREATE TABLE IF NOT EXISTS actor_pair (
    Actor1Code String,
    Actor2Code String,
    interactions UInt32,
    avg_goldstein Float32,
    avg_tone Float32,
    avg_quadclass Float32,
    articles UInt64,
    event_summary_text String
) ENGINE = MergeTree()
ORDER BY (Actor1Code, Actor2Code)
""")

print("All GDELT tables created successfully!")
