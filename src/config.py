import os
import yaml
from pathlib import Path
from typing import List, Dict, Any

class PipelineConfig:
    """Loads and validates configuration from YAML file or environment variables."""
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")
        
        with open(self.config_path, "r", encoding="utf-8") as f:
            self._raw: Dict[str, Any] = yaml.safe_load(f)

    @property
    def website_name(self) -> str:
        return self._raw["website"]["name"]

    @property
    def domain(self) -> str:
        return self._raw["website"]["domain"]

    @property
    def gsc_property_uri(self) -> str:
        return self._raw["website"]["gsc_property_uri"]

    @property
    def bing_site_url(self) -> str:
        return self._raw["website"]["bing_site_url"]

    @property
    def brand_keywords(self) -> List[str]:
        return [k.lower().strip() for k in self._raw["website"].get("brand_keywords", [])]

    @property
    def gsc_service_account_path(self) -> str:
        return self._raw["credentials"]["gsc_service_account_json"]

    @property
    def bing_api_key(self) -> str:
        env_var_name = self._raw["credentials"].get("bing_api_key_env_var", "BING_API_KEY")
        # Check environment variable first, then fallback to config value if present
        return os.environ.get(env_var_name, self._raw["credentials"].get("bing_api_key", ""))

    @property
    def databricks_catalog(self) -> str:
        return self._raw.get("databricks", {}).get("catalog", "main")

    @property
    def databricks_schema(self) -> str:
        return self._raw.get("databricks", {}).get("schema", "seo_analytics")

    @property
    def google_table_name(self) -> str:
        return self._raw.get("databricks", {}).get("google_table_name", "silver_google_search")

    @property
    def bing_table_name(self) -> str:
        return self._raw.get("databricks", {}).get("bing_table_name", "silver_bing_search")

    @property
    def lookback_days(self) -> int:
        return self._raw.get("pipeline", {}).get("lookback_days", 30)

    @property
    def row_batch_limit(self) -> int:
        return self._raw.get("pipeline", {}).get("row_batch_limit", 25000)
