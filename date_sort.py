import pandas as pd

INPUT_CSV = "arrests.csv"

DATE_COLUMN = "apprehension_date"
CUTOFF_DATE = "2025-01-20"

OUT_BEFORE = "arrests_pre.csv"
OUT_AFTER  = "arrests_after.csv"

df = pd.read_csv(INPUT_CSV, dtype="string", low_memory=False)

df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")

cutoff = pd.to_datetime(CUTOFF_DATE)

before = df[df[DATE_COLUMN] < cutoff]
before.to_csv(OUT_BEFORE, index=False)
after  = df[df[DATE_COLUMN] >= cutoff]
after.to_csv(OUT_AFTER, index=False)

print(f"Total rows: {len(df):,}")
print(f"Before {CUTOFF_DATE}: {len(before):,}")
print(f"On/After {CUTOFF_DATE}: {len(after):,}")
print(f"Rows with invalid dates: {df[DATE_COLUMN].isna().sum():,}")