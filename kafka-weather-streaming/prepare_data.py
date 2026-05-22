import pandas as pd
import os
import glob

# Load all CSV files from the data folder
all_files = glob.glob(os.path.join("data", "*.csv"))

dfs = []
for f in all_files:
    # Environment Canada CSVs have some header rows to skip
    df = pd.read_csv(f, skiprows=0, encoding='latin-1')
    dfs.append(df)

# Combine all months
df = pd.concat(dfs, ignore_index=True)

print("Columns found:", df.columns.tolist())
print("Shape:", df.shape)
print(df.head(3))