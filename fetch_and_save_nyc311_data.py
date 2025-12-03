import requests
import json

def fetch_and_save_nyc311_data(api_url, output_file):
    try:
        response = requests.get(api_url, stream=True)
        response.raise_for_status()

        data = response.json()  # load JSON

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"Saved data to: {output_file}")
    except Exception as e:
        print("Error:", e)


# NYC 311 dataset (28+ MILLION rows)
# This query fetches the first 1 million rows as an example
url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=1000000"

fetch_and_save_nyc311_data(url, "nyc_311_first_1m.json")
