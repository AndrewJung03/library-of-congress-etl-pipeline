import json
import os
import requests
from .logger import get_logger 

raw_data_saved_dir = "data/raw"

logger = get_logger("fetch_from_api")


def fetch_from_api(collection="newspapers", max_pages=2, delay=1):
    """
    Fetch results from LOC API and merge into one JSON file.
    @param max_pages: how many pages to fetch
    @param delay: delay between requests
    """

    logger.info(f"Starting API fetch for collection='{collection}', max_pages={max_pages}")

    base_url = f"https://www.loc.gov/{collection}/?fo=json"
    url = base_url
    all_results = []

    for page in range(1, max_pages + 1):

        print(f"Fetching page {page}: {url}")
        logger.info(f"Fetching page {page}: {url}")

        try:
            resp = requests.get(url)
        except Exception as e:
            logger.error(f"Network error while requesting page {page}: {e}")
            raise

        if resp.status_code != 200:
            logger.error(f"Request failed with status {resp.status_code} for URL {url}")
            raise Exception(f"Request failed:{resp.status_code}")

        data = resp.json()
        results = data.get("results", [])

        logger.info(f"Retrieved {len(results)} results on page {page}")
        all_results.extend(results)

        pagination = data.get("pagination", {})
        url = pagination.get("next")

        if not url:
            print("No more pages.")
            logger.info("No more pages returned by API.")
            break



    # Save combined JSON
    output_path = os.path.join(raw_data_saved_dir, f"{collection}_raw.json")

    try:
        with open(output_path, "w") as f:
            json.dump(all_results, f, indent=4)
    except Exception as e:
        logger.error(f"Failed to write JSON output: {e}")
        raise

    print(f"Saved {len(all_results)} total records to {output_path}")
    logger.info(f"Saved {len(all_results)} total records to {output_path}")

    return output_path


if __name__ == "__main__":
    fetch_from_api()
