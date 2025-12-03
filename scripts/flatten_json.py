import pandas as pd

def flatten_json(df):
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