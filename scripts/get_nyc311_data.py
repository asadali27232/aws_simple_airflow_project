import requests
import json

def get_nyc311_data(
        api_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=1000000",
        output_file="s3://nyc311-airflow-data-bucket/raw/nyc_311_first_1m.json"
        ):
    try:
        response = requests.get(api_url, stream=True)
        response.raise_for_status()

        data = response.json()  # load JSON

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"Saved data to: {output_file}")
    except Exception as e:
        print("Error:", e)