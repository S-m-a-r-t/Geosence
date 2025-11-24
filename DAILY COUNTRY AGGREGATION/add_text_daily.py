import duckdb

INPUT = "D:\\geosence\\DAILY COUNTRY AGGREGATION\\country_daily_agg.parquet"
OUTPUT = "D:\\geosence\\DAILY COUNTRY AGGREGATION\\country_daily_with_text.parquet"

con = duckdb.connect()

con.execute(f"""
COPY (
    SELECT
        *,
        CONCAT(
            'On ', date, ' in ', COALESCE(country, 'Unknown'), 
            ', there were ', total_events, ' events. ',
            'Average tone: ', ROUND(avg_tone, 3),
            ', Goldstein: ', ROUND(avg_goldstein, 3),
            '. Violent events: ', violent_events,
            ', verbal conflict: ', verbal_conflict,
            ', cooperation events: ', cooperation_events,
            '. Total mentions: ', total_mentions,
            ', articles: ', total_articles, '.'
        ) AS event_summary_text
    FROM read_parquet('{INPUT}')
)
TO '{OUTPUT}' (FORMAT PARQUET);
""")

print("Daily summary text file created:", OUTPUT)
