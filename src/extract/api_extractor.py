"""
API Extractor Module
Extracts real-time Air Quality data from the WAQI REST API (World Air Quality Index).
Saves raw JSON payloads with audit metadata into the Bronze layer.
"""

import json
import requests
import datetime
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import config

class APIExtractor:
    """
    Handles REST API extraction from WAQI service.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("APIExtractor")
        self.token = config.API_TOKEN
        self.bronze_dir = config.BRONZE_API_DIR
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def extract(self) -> Dict[str, Any]:
        """
        Executes complete API extraction:
        1. Queries the target URL (A07441)
        2. Normalizes / fallbacks to station @7441 if station error occurs
        3. Queries Colombian regional air quality network stations
        4. Persists raw responses to Bronze layer with audit metadata
        """
        self.logger.info("Starting REST API extraction from WAQI...")
        extraction_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        results = {
            "metadata": {
                "extracted_at": extraction_timestamp,
                "api_source": "World Air Quality Index (WAQI)",
                "target_url": config.API_TARGET_URL
            },
            "target_station_raw": None,
            "resolved_station_raw": None,
            "colombia_stations_raw": []
        }

        # 1. Target URL extraction
        try:
            self.logger.info(f"Querying target endpoint: {config.API_TARGET_URL}")
            resp = requests.get(config.API_TARGET_URL, timeout=15)
            target_json = resp.json()
            results["target_station_raw"] = {
                "url": config.API_TARGET_URL,
                "http_status": resp.status_code,
                "response": target_json
            }
            
            # Check if target returned an error or no such station
            if target_json.get("status") != "ok" or target_json.get("data") == "no such station":
                self.logger.warning(
                    f"Target URL returned '{target_json.get('data')}'. Initiating auto-resolution to station '{config.API_FALLBACK_STATION_ID}'..."
                )
                fallback_resp = requests.get(config.API_FALLBACK_URL, timeout=15)
                resolved_json = fallback_resp.json()
                results["resolved_station_raw"] = {
                    "url": config.API_FALLBACK_URL,
                    "http_status": fallback_resp.status_code,
                    "response": resolved_json
                }
            else:
                results["resolved_station_raw"] = results["target_station_raw"]
        except Exception as e:
            self.logger.error(f"Error querying target endpoint: {e}")
            # Attempt fallback directly
            try:
                fallback_resp = requests.get(config.API_FALLBACK_URL, timeout=15)
                results["resolved_station_raw"] = {
                    "url": config.API_FALLBACK_URL,
                    "http_status": fallback_resp.status_code,
                    "response": fallback_resp.json()
                }
            except Exception as fe:
                self.logger.error(f"Fallback endpoint also failed: {fe}")

        # 2. Extract Colombian network stations to complement national GHG inventory
        self.logger.info("Extracting Colombian monitoring stations to complement environmental inventory...")
        for city in config.COLOMBIA_SEARCH_KEYWORDS:
            try:
                search_url = f"{config.API_BASE_SEARCH_URL}/?token={self.token}&keyword={city}"
                s_resp = requests.get(search_url, timeout=15)
                s_data = s_resp.json()
                if s_data.get("status") == "ok":
                    stations_found = s_data.get("data", [])
                    self.logger.info(f"Found {len(stations_found)} stations for '{city}'")
                    # Fetch detailed feed for top stations in each city
                    for station in stations_found[:3]:
                        uid = station.get("uid")
                        if uid:
                            feed_url = f"{config.API_BASE_FEED_URL}/@{uid}/?token={self.token}"
                            feed_resp = requests.get(feed_url, timeout=10)
                            feed_data = feed_resp.json()
                            if feed_data.get("status") == "ok":
                                results["colombia_stations_raw"].append({
                                    "uid": uid,
                                    "city_keyword": city,
                                    "url": feed_url,
                                    "http_status": feed_resp.status_code,
                                    "response": feed_data
                                })
            except Exception as e:
                self.logger.warning(f"Error fetching stations for city '{city}': {e}")

        # 3. Save raw extract to Bronze Storage
        raw_output_file = self.bronze_dir / "raw_waqi_api_extract.json"
        with open(raw_output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        total_extracted = 1 + len(results["colombia_stations_raw"])
        self.logger.info(f"Bronze API Extraction complete. Saved {total_extracted} station records to {raw_output_file}")
        return results
