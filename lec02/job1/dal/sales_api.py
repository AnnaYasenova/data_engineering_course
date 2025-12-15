from typing import List, Dict, Any
import requests, os

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'

AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")

def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Fetches all pages of sales data from API for a given date.

    Args:
        date: Date in format YYYY-MM-DD

    Returns:
        List of all sales records from all pages
    """
    all_data = []
    page = 1

    headers = {
        "Authorization": AUTH_TOKEN
    }

    while True:
        url = f"{API_URL}/sales"
        params = {
            "date": date,
            "page": page
        }

        print(f"Fetching page {page} for date {date}...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 404:
            print(f"No more data found at page {page}")
            break
        elif response.status_code == 401:
            raise ValueError("Invalid AUTH_TOKEN - authentication failed")
        elif response.status_code != 200:
            raise ValueError(f"API request failed with status {response.status_code}: {response.text}")

        page_data = response.json()

        if not page_data or len(page_data) == 0:
            print(f"No data returned on page {page}")
            break

        all_data.extend(page_data)
        print(f"Retrieved {len(page_data)} records from page {page}")
        page += 1

    print(f"Total records fetched: {len(all_data)}")
    return all_data