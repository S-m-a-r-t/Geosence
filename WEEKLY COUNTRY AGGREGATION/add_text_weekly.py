import duckdb

INPUT = "D:\\geosence\\WEEKLY COUNTRY AGGREGATION\\country_weekly_agg.parquet"
OUTPUT = "D:\\geosence\\WEEKLY COUNTRY AGGREGATION\\country_weekly_with_text.parquet"

con = duckdb.connect()

con.execute(f"""
COPY (
    SELECT
        *,
        CONCAT(
            'In week ', week_of_month, ' of ', yearmonth, 
            ' for ', COALESCE(country, 'Unknown'), 
            ', total events: ', total_events,
            ', Avg Tone: ', ROUND(avg_tone, 3),
            ', Avg Goldstein: ', ROUND(avg_goldstein, 3),
            ', Total mentions: ', total_mentions,
            ', Total articles: ', total_articles, '.'
        ) AS event_summary_text
    FROM read_parquet('{INPUT}')
)
TO '{OUTPUT}' (FORMAT PARQUET);
""")

print("Weekly summary text file created:", OUTPUT)
