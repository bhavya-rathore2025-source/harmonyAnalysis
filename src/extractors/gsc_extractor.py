"""Google Search Console API Extractor.

Pulls daily query and page performance metrics using a Service Account JSON.
Pure Python - returns raw JSON records.
"""
from typing import List, Dict, Any
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

class GSCExtractor:
    def __init__(self, property_uri: str, credentials_path: str):
        self.property_uri = property_uri
        self.credentials_path = credentials_path
        self._service = None

    def _get_service(self):
        if not self._service:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(
                    f"GSC service account key not found at: {self.credentials_path}. "
                    "Please download it from Google Cloud Console and place it in the credentials/ folder."
                )
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=SCOPES
            )
            self._service = build("searchconsole", "v1", credentials=creds)
        return self._service

    def fetch_performance_data(
        self,
        start_date: str,
        end_date: str,
        row_limit: int = 25000
    ) -> List[Dict[str, Any]]:
        """Fetches raw performance metrics grouped by date, query, and page."""
        service = self._get_service()
        request_body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date", "query", "page"],
            "rowLimit": row_limit,
            "startRow": 0
        }

        response = service.searchanalytics().query(
            siteUrl=self.property_uri, body=request_body
        ).execute()

        rows = response.get("rows", [])
        records: List[Dict[str, Any]] = []
        for r in rows:
            keys = r.get("keys", ["", "", ""])
            records.append({
                "date": keys[0],
                "query": keys[1],
                "page": keys[2],
                "clicks": int(r.get("clicks", 0)),
                "impressions": int(r.get("impressions", 0)),
                "ctr": float(r.get("ctr", 0.0)),
                "position": float(r.get("position", 0.0))
            })

        return records
