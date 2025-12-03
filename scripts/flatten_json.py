import pandas as pd
import json
from pathlib import Path

def flatten_json(
        input_file="s3://nyc311-airflow-data-bucket/raw/nyc_311_first_1m.json",
        output_file="s3://nyc311-airflow-data-bucket/raw/nyc_311_flattened.json"
    ):
    """
    Reads a JSON file, flattens any nested dict columns, 
    and saves the flattened data back to a JSON file.
    """
    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Load JSON into DataFrame
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Detect and flatten nested dict columns
    cols_to_flatten = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
    for col in cols_to_flatten:
        flattened = pd.json_normalize(df[col])
        flattened.columns = [f"{col}_{sub}" for sub in flattened.columns]
        df = df.drop(columns=[col]).join(flattened)

    # Save flattened DataFrame back to JSON
    df.to_json(output_file, orient="records", indent=4, date_format="iso")

    print(f"Flattened JSON saved to: {output_file}")
