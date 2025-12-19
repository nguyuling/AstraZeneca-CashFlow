import duckdb
import polars as pl
import pandas as pd

con = duckdb.connect()

# joins the 3 other .csv files
master_query = """
SELECT 
    m.*, 
    c.Country, 
    c.Currency AS Country_Currency,
    l."ID Category Cat Order" AS Category_Details,
    e."Rate (USD)" AS Live_FX_Rate
FROM 'Data-Main.csv' m
LEFT JOIN 'Others-Country Mapping.csv' c ON m."File-Month Name" = c.Code
LEFT JOIN 'Others-Category Linkage.csv' l ON m.Category = l."Category Names"
LEFT JOIN 'Others-Exchange Rate.csv' e ON m.Curr = e.Code
"""

# execute the SQL
df = con.execute(master_query).pl()

# create the week column.
df = df.with_columns([
    pl.col("Psing Date").str.to_date("%m/%d/%y").alias("Date"),
])

df_weekly = df.with_columns([
    pl.col("Date").dt.truncate("1w").alias("Week_Start")
])

# pull the starting balance from the cash balance .csv
df_balance = pl.read_csv("Data-Cash Balance.csv")

# join the starting balance to the main data
df_final = df_weekly.join(
    df_balance, 
    left_on="File-Month Name", 
    right_on="Name", 
    how="left"
)

# calc ending balance = starting carryforward + cumulative sum of (in usd)
df_final = df_final.sort(["File-Month Name", "Date"])
df_final = df_final.with_columns([
    (pl.col("Carryforward Balance (USD)") + 
     pl.col("Amount in USD").cum_sum().over("File-Month Name")
    ).alias("Running_Ending_Balance")
])

# 6. export to csv for Power BI
final_pandas_df = df_final.to_pandas()
final_pandas_df.to_csv("Master_AstraZeneca_Data.csv", index=False)
print("Process Complete! Your Master CSV is ready for Power BI.")