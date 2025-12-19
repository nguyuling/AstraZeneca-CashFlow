import duckdb
import polars as pl
import pandas as pd

# setup DuckDB Connection
con = duckdb.connect()

# join 3 others.csv into main.csv
master_query = """
SELECT 
    m."File-Month Name", 
    m."Psing Date", 
    m."Amount in USD", 
    m.Category,
    c.Country,
    l."ID Category Cat Order" AS Category_Detail,
    b."Carryforward Balance (USD)" AS Starting_Balance
FROM 'Data - Main.csv' m
LEFT JOIN 'Others - Country Mapping.csv' c ON m."File-Month Name" = c.Code
LEFT JOIN 'Others - Category Linkage.csv' l ON m.Category = l."Category Names"
LEFT JOIN 'Data - Cash Balance.csv' b ON m."File-Month Name" = b.Name
"""

# execute the sql using DuckDB and load into Polars DataFrame
df = con.execute(master_query).pl()

# standardize dates and create weekly buckets
# calculate the running cash balance per entity
df_processed = (
    df
    .with_columns([
        pl.col("Psing Date").str.to_date("%m/%d/%y").alias("Date"),
    ])
    .with_columns([
        pl.col("Date").dt.truncate("1w").alias("Forecast_Week")
    ])
    .sort(["File-Month Name", "Date"])
    .with_columns([
        # Running Balance = Carryforward + Cumulative Sum of Net Cash Flow
        (pl.col("Starting_Balance") + 
         pl.col("Amount in USD").cum_sum().over("File-Month Name")
        ).alias("Ending_Cash_Balance")
    ])
)

# export the final DataFrame to .csv for Power BI
df_processed.to_pandas().to_csv("AstraZeneca_Master_Data.csv", index=False)
print("Process Complete. Optimized CSV generated for Power BI.")