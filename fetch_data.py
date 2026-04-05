import os
import json
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from collections import Counter
from datetime import datetime

SHEET_ID = "1aWTc7UJnMdgO3OVaGk73F7ZzL51K7J2pmJIZ_7aArIY"
TAB_NAME = "External_db"
OUTPUT_JSON = "data/weekly_summary.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]

def get_client():
    key_json = os.environ.get("GSHEET_KEY")
    if not key_json:
        raise ValueError("GSHEET_KEY environment variable not set")
    creds_dict = json.loads(key_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def fetch_sheet():
    client = get_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    print(f"Fetched {len(df)} rows from '{TAB_NAME}'")
    return df

def get_egyptian_week(dates):
    """
    Egyptian week: Sunday=start, Saturday=end.
    Shift date back 1 day so Sunday acts as week start in ISO calculation.
    """
    shifted = dates - pd.Timedelta(days=1)
    year = shifted.dt.isocalendar().year.astype(int)
    week = shifted.dt.isocalendar().week.astype(int)
    return year, week

def process(df):
    df = df[df.get("is_test", "") != "Yes"] if "is_test" in df.columns else df
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"])

    # Apply Egyptian week (Sunday-Saturday)
    df["eg_year"], df["eg_week"] = get_egyptian_week(df["Timestamp"])

    # Filter current year
    current_year = datetime.now().year
    df = df[df["eg_year"] == current_year]

    result = {}
    for wk in sorted(df["eg_week"].unique()):
        wdf = df[df["eg_week"] == wk]

        # Week date range label (actual min/max dates in data)
        week_start = wdf["Timestamp"].min().strftime("%b %d")
        week_end = wdf["Timestamp"].max().strftime("%b %d")

        roles = Counter()
        for r in wdf.get("Roles", pd.Series(dtype=str)).dropna():
            for role in str(r).split(","):
                roles[role.strip()] += 1

        locs = Counter()
        for l in wdf.get("Preferred work locations", pd.Series(dtype=str)).dropna():
            for loc in str(l).split(","):
                locs[loc.strip()] += 1

        sal = pd.to_numeric(
            wdf.get("Expected monthly salary figure", pd.Series(dtype=float)),
            errors="coerce"
        ).dropna()

        result[int(wk)] = {
            "count": len(wdf),
            "week_range": f"{week_start} – {week_end}",
            "roles": dict(roles.most_common()),
            "top_locations": dict(locs.most_common(4)),
            "salary_avg": int(sal.mean()) if len(sal) else 0,
            "salary_min": int(sal.min()) if len(sal) else 0,
            "salary_max": int(sal.max()) if len(sal) else 0,
            "salary_pool": int(sal.sum()) if len(sal) else 0,
        }

    return result

def main():
    os.makedirs("data", exist_ok=True)
    df = fetch_sheet()
    summary = process(df)

    with open(OUTPUT_JSON, "w") as f:
        json.dump({
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "week_standard": "Egyptian (Sunday-Saturday)",
            "weeks": summary
        }, f, indent=2)

    print(f"Saved summary to {OUTPUT_JSON}")
    print(f"Weeks processed: {list(summary.keys())}")
    for wk, data in summary.items():
        print(f"  Wk {wk} ({data['week_range']}): {data['count']} entries")

if __name__ == "__main__":
    main()
