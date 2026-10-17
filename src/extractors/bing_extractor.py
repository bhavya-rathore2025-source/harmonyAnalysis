"""Bing Webmaster Tools API Extractor.

Fetches performance stats using the official REST API and API Key.
Pure Python - returns raw JSON records.
"""
from typing import List, Dict, Any
import requests

BING_BASE_URL = "https://ssl.bing.com/webmaster/api.svc/json"

class BingExtractor:
    def __init__(self, site_url: str, api_key: str):
        self.site_url = site_url
        self.api_key = api_key

    def fetch_query_stats(self) -> List[Dict[str, Any]]:
        """Pulls raw query performance data from Bing Webmaster Tools API."""
        if not self.api_key:
            raise ValueError("Bing API key is empty. Set BING_API_KEY env var or provide it in config.")

        endpoint = f"{BING_BASE_URL}/GetQueryStats"
        params = {
            "siteUrl": self.site_url,
            "apikey": self.api_key
        }

        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get("d", [])

        records: List[Dict[str, Any]] = []
        for item in results:
            records.append({
                "date": item.get("Date", ""),
                "query": item.get("Query", ""),
                "clicks": int(item.get("Clicks", 0)),
                "impressions": int(item.get("Impressions", 0)),
                "avg_position": float(item.get("AvgPosition", 0.0)),
                "avg_ctr": float(item.get("Ctr", 0.0))
            })

        return records
