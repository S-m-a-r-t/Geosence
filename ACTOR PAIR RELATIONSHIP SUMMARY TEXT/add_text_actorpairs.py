import duckdb

INPUT = "D:\\geosence\\ACTOR PAIR RELATIONSHIP SUMMARY TEXT\\actor_pair_agg.parquet"
OUTPUT = "D:\\geosence\\ACTOR PAIR RELATIONSHIP SUMMARY TEXT\\actor_pair_with_text.parquet"

con = duckdb.connect()

con.execute(f"""
COPY (
    SELECT
        *,
        CONCAT(
            'Interactions between ', COALESCE(Actor1Code, 'Unknown'),
            ' and ', COALESCE(Actor2Code, 'Unknown'),
            ': ', interactions, ' total events. ',
            'Average tone: ', ROUND(avg_tone, 3),
            ', Average Goldstein: ', ROUND(avg_goldstein, 3),
            ', Average QuadClass: ', ROUND(avg_quadclass, 3),
            ', Total articles: ', articles, '.'
        ) AS event_summary_text
    FROM read_parquet('{INPUT}')
)
TO '{OUTPUT}' (FORMAT PARQUET);
""")

print("Actor pair summary text file created:", OUTPUT)
