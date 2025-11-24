import duckdb

INPUT_PARQUET = "gdelt_cleaned.parquet"

con = duckdb.connect()

print(" Creating DAILY COUNTRY aggregation...")

con.execute(f"""
COPY (
    SELECT
        ActionGeo_CountryCode AS country,
        SQLDATE AS date,

        COUNT(*) AS total_events,
        AVG(CAST(QuadClass AS DOUBLE)) AS avg_quadclass,
        AVG(CAST(GoldsteinScale AS DOUBLE)) AS avg_goldstein,
        AVG(CAST(AvgTone AS DOUBLE)) AS avg_tone,

        SUM(CAST(NumMentions AS BIGINT)) AS total_mentions,
        SUM(CAST(NumArticles AS BIGINT)) AS total_articles,

        COUNT(CASE WHEN QuadClass='4' THEN 1 END) AS violent_events,
        COUNT(CASE WHEN QuadClass='2' THEN 1 END) AS verbal_conflict,
        COUNT(CASE WHEN QuadClass='1' THEN 1 END) AS cooperation_events

    FROM read_parquet('{INPUT_PARQUET}')
    GROUP BY country, date
) TO 'country_daily_agg.parquet' (FORMAT PARQUET);
""")

print(" Daily country-level aggregation saved: country_daily_agg.parquet")


print(" Creating WEEKLY COUNTRY aggregation...")

con.execute(f"""
COPY (
    SELECT
        ActionGeo_CountryCode AS country,
        SUBSTR(SQLDATE, 1, 6) AS yearmonth,
        FLOOR((CAST(SUBSTR(SQLDATE, 7, 2) AS INT) - 1) / 7) AS week_of_month,

        COUNT(*) AS total_events,
        AVG(CAST(QuadClass AS DOUBLE)) AS avg_quadclass,
        AVG(CAST(GoldsteinScale AS DOUBLE)) AS avg_goldstein,
        AVG(CAST(AvgTone AS DOUBLE)) AS avg_tone,

        SUM(CAST(NumMentions AS BIGINT)) AS total_mentions,
        SUM(CAST(NumArticles AS BIGINT)) AS total_articles
    FROM read_parquet('{INPUT_PARQUET}')
    GROUP BY country, yearmonth, week_of_month
) TO 'country_weekly_agg.parquet' (FORMAT PARQUET);
""")

print(" Weekly country-level aggregation saved: country_weekly_agg.parquet")


print(" Creating ACTOR PAIR aggregation...")

con.execute(f"""
COPY (
    SELECT
        Actor1Code,
        Actor2Code,

        COUNT(*) AS interactions,
        AVG(CAST(GoldsteinScale AS DOUBLE)) AS avg_goldstein,
        AVG(CAST(AvgTone AS DOUBLE)) AS avg_tone,
        AVG(CAST(QuadClass AS DOUBLE)) AS avg_quadclass,

        SUM(CAST(NumArticles AS BIGINT)) AS articles
    FROM read_parquet('{INPUT_PARQUET}')
    WHERE Actor1Code IS NOT NULL AND Actor2Code IS NOT NULL
    GROUP BY Actor1Code, Actor2Code
) TO 'actor_pair_agg.parquet' (FORMAT PARQUET);
""")

print(" Actor pair aggregation saved: actor_pair_agg.parquet")

print("\n ALL AGGREGATIONS COMPLETED SUCCESSFULLY!")
