import pandas as pd
import json

KEY = "unique_identifier"

arrests  = pd.read_csv("arrests_after.csv", dtype="string", low_memory=True)
detainers = pd.read_csv("detainers.csv", dtype="string", low_memory=True)
stints   = pd.read_csv("stints_1.csv", dtype="string", low_memory=True)
stays    = pd.read_csv("stays.csv", dtype="string", low_memory=True)

for df in [arrests, detainers, stints, stays]:
    df[KEY] = df[KEY].str.strip()

STINT_DATE = "book_in_date_time"
STAY_DATE  = "stay_book_in_date_time"
stints[STINT_DATE] = pd.to_datetime(stints[STINT_DATE], errors="coerce")
stays[STAY_DATE]   = pd.to_datetime(stays[STAY_DATE], errors="coerce")

stints = stints.sort_values([KEY, STINT_DATE])
def pack_stints(g):
    return json.dumps(g.drop(columns=[KEY]).to_dict("records"), default=str)
stints_agg = (
    stints
    .groupby(KEY)
    .apply(pack_stints)
    .reset_index(name="stints_json")
)

stays = stays.sort_values([KEY, STAY_DATE])
def pack_stays(g):
    return json.dumps(g.drop(columns=[KEY]).to_dict("records"), default=str)
stays_agg = (
    stays
    .groupby(KEY)
    .apply(pack_stays)
    .reset_index(name="stays_json")
)
detainers = detainers.drop_duplicates(subset=[KEY])

master = arrests.merge(detainers, on=KEY, how="left")
master = master.merge(stints_agg, on=KEY, how="left")
master = master.merge(stays_agg, on=KEY, how="left")

master.to_csv("ice_after_master.csv", index=False)

print("Rows:", len(master))
print("Stints attached:", master["stints_json"].notna().sum())
print("Stays attached:", master["stays_json"].notna().sum())