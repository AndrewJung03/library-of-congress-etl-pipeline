import json
import os
import time

import requests

raw_data_saved_dir = "data/raw"


def fetch_from_api(collection="newspapers", max_pages=20, delay=1):
    """
    Fetch results from LOC API and merge into one JSON file.
    @param max_pages: how many pages to fetch
    @param delay: delay between requests
    """
    base_url = f"https://www.loc.gov/{collection}/?fo=json"
    url = base_url
    all_results = []

    for page in range(1, max_pages + 1):
        # fetch
        print(f"Fetching page {page}: {url}")
        resp = requests.get(url)
        # if fails
        if resp.status_code != 200:
            raise Exception(f"Request failed:{resp.status_code}")
        # if not fail
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)
        # if no more pages
        pagination = data.get("pagination", {})
        url = pagination.get("next")
        # no more pages
        if not url:
            print("No more pages.")
            break

    # Save combined JSON
    output_path = os.path.join(raw_data_saved_dir, f"{collection}_raw.json")
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=4)
    print(f"Saved {len(all_results)} total records to {output_path}")
    return output_path



