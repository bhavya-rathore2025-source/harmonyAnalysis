"""Raw API Extraction Runner (Pure Python - No Pandas).

Fetches raw search data from GSC and Bing APIs and saves directly to
JSON files in data/raw/ ready for Databricks SQL ingestion.
"""
import json
from datetime import date, timedelta
from pathlib import Path
from src.config import PipelineConfig
from src.extractors.gsc_extractor import GSCExtractor
from src.extractors.bing_extractor import BingExtractor

def run():
    print("=" * 60)
    print(">> Extracting Raw Search Data (Pure Python)")
    print("=" * 60)

    config = PipelineConfig("config/config.yaml")
    print(f"Target site: {config.website_name} ({config.domain})")

    end_date = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d") # GSC 2-day reporting lag
    start_date = (date.today() - timedelta(days=config.lookback_days)).strftime("%Y-%m-%d")
    print(f"Date range: {start_date} to {end_date}")

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Google Search Console Raw Ingestion
    print("\n[1/2] Fetching Google Search Console raw data...")
    try:
        gsc = GSCExtractor(config.gsc_property_uri, config.gsc_service_account_path)
        gsc_records = gsc.fetch_performance_data(start_date, end_date, config.row_batch_limit)
        gsc_out_file = raw_dir / f"{config.website_name}_gsc_raw.json"
        
        with open(gsc_out_file, "w", encoding="utf-8") as f:
            json.dump(gsc_records, f, indent=2)
            
        print(f"  -> Extracted {len(gsc_records)} records.")
        print(f"  -> Saved raw JSON to: {gsc_out_file}")
    except Exception as e:
        print(f"  [ERROR] GSC extract skipped or failed: {e}")

    # 2. Bing Webmaster Tools Raw Ingestion
    print("\n[2/2] Fetching Bing Webmaster Tools raw data...")
    try:
        bing = BingExtractor(config.bing_site_url, config.bing_api_key)
        bing_records = bing.fetch_query_stats()
        bing_out_file = raw_dir / f"{config.website_name}_bing_raw.json"
        
        with open(bing_out_file, "w", encoding="utf-8") as f:
            json.dump(bing_records, f, indent=2)
            
        print(f"  -> Extracted {len(bing_records)} records.")
        print(f"  -> Saved raw JSON to: {bing_out_file}")
    except Exception as e:
        print(f"  [INFO] Bing extract skipped or failed: {e}")

    print("\n>> Raw extraction complete. Data is ready for Databricks SQL transformations!")

if __name__ == "__main__":
    run()
