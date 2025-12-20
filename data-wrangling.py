import polars as pl

# 1. load files
main = pl.scan_csv(
    "Data-Main.csv", 
    schema_overrides={
        "Document Header Text": pl.String,
        "DocumentNo": pl.String,
        "Assignment": pl.String,
        "Reference": pl.String,
        "Amount in USD": pl.String,
        "Amt in loc.cur.": pl.String,
        "Amount in doc. curr.": pl.String,
    },
    infer_schema_length=10000 
)
balance = pl.scan_csv("Data-CashBalance.csv")
linkage = pl.scan_csv("Others-CategoryLinkage.csv").rename({"Category": "Flow_Direction"})
mapping = pl.scan_csv("Others-CountryMapping.csv")

df_final = (
    main
    # joins
    .join(mapping, left_on="Name", right_on="Code", how="left")
    .join(linkage, left_on="Category", right_on="Category Names", how="left")
    .join(balance, left_on="Name", right_on="Name", how="left")
    
    # data cleaning
    .with_columns([
        # clean currency and cast to numbers
        pl.col("Amount in USD").str.replace_all(",", "").cast(pl.Float64),
        pl.col("Carryforward Balance (USD)").str.strip_chars().str.replace_all(",", "").cast(pl.Float64),
        
        # parse date
        pl.col("Pstng Date").str.to_date("%m/%d/%Y").alias("Date")
    ])
    
    # calculations
    .with_columns([
        pl.col("Date").dt.truncate("1w").alias("Forecast_Week")
    ])
    .sort(["Name", "Date"])
    .with_columns([
        (pl.col("Carryforward Balance (USD)") + 
         pl.col("Amount in USD").cum_sum().over("Name")
        ).alias("Ending_Cash_Balance")
    ])
    
    # select only useful columns
    .select([
        "Name",                     # Entity Code (TW10 etc)
        "Country",                  # From Mapping
        "Date",                     # Cleaned Posting Date
        "Forecast_Week",            # Weekly buckets for 6-month forecast
        "Amount in USD",            # Net Cash Flow
        "Category",                 # Transaction category (AP, AR, etc)
        "Flow_Direction",           # Inflow vs Outflow
        "Ending_Cash_Balance",      # The most important KPI
        "DocumentNo",               # For Anomaly Detection
        "Document Header Text",     # For Storytelling/Anomalies
        "Reference",                # For Reconciliation
        "Assignment"                # For Reconciliation
    ])
    
    .collect()
)

# final export
df_final.write_csv("AstraZeneca_Master_Data.csv")
print("Success! The lean Master Data file is ready for Power BI.")