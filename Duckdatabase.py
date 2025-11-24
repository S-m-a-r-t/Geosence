import duckdb

CSV_PATH = 'd:/geosence/gdelt_arranged/**/*.CSV'

conn = duckdb.connect("gdelt_v1_clean.duckdb")

print("Loading raw GDELT 1.0 CSVs (58 columns, forgiving parser)...")

conn.execute(f"""
CREATE OR REPLACE VIEW raw AS
SELECT *
FROM read_csv_auto(
    '{CSV_PATH}',
    HEADER = FALSE,
    ALL_VARCHAR = TRUE,
    NULL_PADDING = TRUE,
    IGNORE_ERRORS = TRUE
);
""")

print("Cleaning and selecting required columns...")

clean_sql = """
CREATE OR REPLACE TABLE gdelt_clean AS
SELECT
    column00 AS GLOBALEVENTID,
    column01 AS SQLDATE,

    column05 AS Actor1Code,
    column06 AS Actor1Name,

    column15 AS Actor2Code,
    column16 AS Actor2Name,

    column26 AS EventCode,
    column27 AS EventBaseCode,
    column28 AS EventRootCode,

    column29 AS QuadClass,
    column30 AS GoldsteinScale,
    column31 AS NumMentions,
    column33 AS NumArticles,
    column34 AS AvgTone,

    column53 AS ActionGeo_CountryCode,
    column55 AS ActionGeo_Lat,
    column56 AS ActionGeo_Long
FROM raw;
"""

conn.execute(clean_sql)

print("Exporting to Parquet...")

conn.execute("""
COPY (SELECT * FROM gdelt_clean)
TO 'gdelt_cleaned.parquet'
(FORMAT PARQUET);
""")

print("DONE!! Cleaned GDELT 1.0 dataset saved as gdelt_cleaned.parquet")
