import json
import pandas as pd


import pandas as pd

def flatten_nested_columns(df):
    """
    Detects columns that contain dict/json objects,
    flattens them, and merges them back into the DataFrame.
    """
    cols_to_flatten = []

    # Find columns that contain dicts
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            cols_to_flatten.append(col)

    # Flatten each dict column
    for col in cols_to_flatten:
        # Normalize the nested dict column
        flattened = pd.json_normalize(df[col])
        flattened.columns = [f"{col}_{sub}" for sub in flattened.columns]

        # Drop original nested column and join flattened data
        df = df.drop(columns=[col]).join(flattened)

    return df


def clean_nyc311_to_csv(input_file, output_file):
    # Load JSON into memory
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Convert to DataFrame
    df = pd.DataFrame(raw_data)

    # flattern nested columns
    df = flatten_nested_columns(df)

    # ---- 1. Standardize Column Names ----
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # ---- 2. Remove Duplicates ----
    df.drop_duplicates(inplace=True)

    # ---- 3. Convert Date Columns ----
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # ---- 4. Numeric Conversions ----
    numeric_cols = ["latitude", "longitude", "community_board", "incident_zip"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---- 5. Fill Missing Values ----
    fill_defaults = {
        "agency": "Unknown",
        "complaint_type": "Not Specified",
        "descriptor": "Not Specified",
        "borough": "Unknown",
    }
    for col, default_val in fill_defaults.items():
        if col in df.columns:
            df[col].fillna(default_val, inplace=True)

    # ---- 6. Keep Only Important Columns (optional) ----
    useful_cols = [
        "unique_key",
        "status",
        "created_date",
        "closed_date",
        "agency",
        "complaint_type",
        "descriptor",
        "incident_zip",
        "incident_address",
        "borough",
        "latitude",
        "longitude",
        "resolution_description"
    ]

    df = df[[c for c in useful_cols if c in df.columns]]

    # ---- 7. Save as CSV ----
    df.to_csv(output_file, index=False)

    print(f"Clean CSV saved as: {output_file}")


# Run it
clean_nyc311_to_csv("nyc_311_first_1m.json", "nyc_311_cleaned.csv")
