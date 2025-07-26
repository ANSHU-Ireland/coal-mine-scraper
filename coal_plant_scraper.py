#!/usr/bin/env python3
"""
Global Coal Plant Tracker Scraper
Extracts comprehensive coal plant data from Global Energy Monitor's tracker
"""

import requests
import pandas as pd
import json
import time
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coal_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class GlobalCoalPlantScraper:
    """Scraper for Global Energy Monitor's Coal Plant Tracker"""
    
    def __init__(self):
        self.base_url = "https://globalenergymonitor.org"
        self.tracker_url = "https://globalenergymonitor.org/projects/global-coal-plant-tracker/tracker/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.data = []
        
    def get_api_endpoint(self) -> Optional[str]:
        """
        Find the API endpoint for coal plant data
        """
        try:
            response = self.session.get(self.tracker_url)
            response.raise_for_status()
            
            # Look for common API patterns in the page source
            content = response.text
            
            # Check for various possible API endpoints
            possible_endpoints = [
                "/api/coal-plants",
                "/api/tracker/coal-plants", 
                "/projects/global-coal-plant-tracker/api/data",
                "/wp-json/gem/v1/coal-plants",
                "/data/coal-plants.json",
                "/tracker-data/coal-plants"
            ]
            
            for endpoint in possible_endpoints:
                full_url = urljoin(self.base_url, endpoint)
                try:
                    test_response = self.session.get(full_url, timeout=10)
                    if test_response.status_code == 200:
                        # Check if response contains JSON data
                        try:
                            data = test_response.json()
                            if isinstance(data, (list, dict)) and data:
                                logger.info(f"Found API endpoint: {full_url}")
                                return full_url
                        except json.JSONDecodeError:
                            continue
                except:
                    continue
            
            # Look for embedded JSON data in the page
            if '"coal' in content.lower() or '"plant' in content.lower():
                # Try to extract JSON from script tags
                import re
                json_pattern = r'var\s+\w+\s*=\s*(\[.*?\]|\{.*?\});'
                matches = re.findall(json_pattern, content, re.DOTALL)
                
                for match in matches:
                    try:
                        data = json.loads(match)
                        if self._validate_coal_data(data):
                            logger.info("Found embedded JSON data in page")
                            return "embedded"
                    except:
                        continue
            
            logger.warning("Could not find API endpoint, will try alternative methods")
            return None
            
        except Exception as e:
            logger.error(f"Error finding API endpoint: {e}")
            return None
    
    def _validate_coal_data(self, data) -> bool:
        """Validate if data contains coal plant information"""
        if not data:
            return False
            
        # Check if it's a list of records
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0] if data else {}
        elif isinstance(data, dict):
            # Might be wrapped in another structure
            first_item = data
        else:
            return False
            
        # Look for coal plant related fields
        coal_indicators = [
            'plant', 'unit', 'capacity', 'coal', 'power', 'mw', 'status',
            'country', 'region', 'owner', 'parent', 'start', 'retire'
        ]
        
        if isinstance(first_item, dict):
            keys_lower = [k.lower() for k in first_item.keys()]
            return any(indicator in ' '.join(keys_lower) for indicator in coal_indicators)
        
        return False
    
    def scrape_from_api(self, api_url: str) -> List[Dict]:
        """Scrape data from API endpoint"""
        try:
            if api_url == "embedded":
                # Extract from page source
                response = self.session.get(self.tracker_url)
                content = response.text
                
                import re
                json_pattern = r'var\s+\w+\s*=\s*(\[.*?\]|\{.*?\});'
                matches = re.findall(json_pattern, content, re.DOTALL)
                
                for match in matches:
                    try:
                        data = json.loads(match)
                        if self._validate_coal_data(data):
                            return self._normalize_data(data)
                    except:
                        continue
                return []
            
            # Try direct API call
            response = self.session.get(api_url)
            response.raise_for_status()
            
            data = response.json()
            return self._normalize_data(data)
            
        except Exception as e:
            logger.error(f"Error scraping from API {api_url}: {e}")
            return []
    
    def scrape_with_pagination(self, base_api_url: str) -> List[Dict]:
        """Handle paginated API responses"""
        all_data = []
        page = 1
        
        while True:
            try:
                # Try different pagination patterns
                pagination_patterns = [
                    f"{base_api_url}?page={page}",
                    f"{base_api_url}?offset={len(all_data)}",
                    f"{base_api_url}?limit=1000&offset={len(all_data)}",
                    f"{base_api_url}&page={page}",
                ]
                
                found_data = False
                for url in pagination_patterns:
                    try:
                        response = self.session.get(url, timeout=30)
                        if response.status_code == 200:
                            data = response.json()
                            
                            if isinstance(data, list):
                                page_data = data
                            elif isinstance(data, dict):
                                # Common API response patterns
                                page_data = data.get('data', data.get('results', data.get('items', [])))
                            else:
                                continue
                            
                            if page_data:
                                all_data.extend(self._normalize_data(page_data))
                                found_data = True
                                logger.info(f"Retrieved page {page}, total records: {len(all_data)}")
                                break
                            
                    except Exception as e:
                        logger.debug(f"Pagination attempt failed for {url}: {e}")
                        continue
                
                if not found_data:
                    break
                    
                page += 1
                time.sleep(1)  # Be respectful
                
                # Safety limit
                if page > 100:
                    logger.warning("Reached pagination limit")
                    break
                    
            except Exception as e:
                logger.error(f"Error in pagination: {e}")
                break
        
        return all_data
    
    def _normalize_data(self, raw_data) -> List[Dict]:
        """Normalize the raw data to standard format"""
        if not raw_data:
            return []
        
        # Handle different data structures
        if isinstance(raw_data, dict):
            # Check if it's a wrapper around the actual data
            for key in ['data', 'results', 'items', 'plants', 'records']:
                if key in raw_data and isinstance(raw_data[key], list):
                    raw_data = raw_data[key]
                    break
            else:
                # Single record
                raw_data = [raw_data]
        
        if not isinstance(raw_data, list):
            return []
        
        normalized_data = []
        
        for item in raw_data:
            if not isinstance(item, dict):
                continue
                
            # Create normalized record with all possible field mappings
            record = self._map_fields(item)
            
            if record and any(record.values()):  # Only add if has some data
                normalized_data.append(record)
        
        return normalized_data
    
    def _map_fields(self, item: Dict) -> Dict:
        """Map various field names to standard format"""
        record = {
            'plant_name': '',
            'unit_name': '',
            'plant_unit_name': '',
            'owner': '',
            'parent_company': '',
            'capacity_mw': '',
            'status': '',
            'start_year': '',
            'retired_year': '',
            'region': '',
            'country_area': '',
            'subnational_unit': '',
            'latitude': '',
            'longitude': '',
            'technology': '',
            'fuel_type': '',
            'announced_year': '',
            'construction_start': '',
            'operating_year': '',
            'mothballed_year': '',
            'cancelled_year': '',
            'wiki_url': ''
        }
        
        # Field mapping dictionary - maps various possible field names to our standard format
        field_mappings = {
            'plant_name': [
                'plant_name', 'plant', 'name', 'facility_name', 'plant_id',
                'plantName', 'Plant Name', 'Plant', 'Facility'
            ],
            'unit_name': [
                'unit_name', 'unit', 'unit_id', 'unitName', 'Unit Name', 'Unit'
            ],
            'plant_unit_name': [
                'plant_unit_name', 'tracker_id', 'id', 'Plant/Unit Name'
            ],
            'owner': [
                'owner', 'Owner', 'owner_company', 'operating_company',
                'operator', 'Operator'
            ],
            'parent_company': [
                'parent_company', 'parent', 'Parent Company', 'Parent',
                'ultimate_owner', 'holding_company'
            ],
            'capacity_mw': [
                'capacity_mw', 'capacity', 'mw', 'MW', 'Capacity (MW)',
                'power_mw', 'rated_capacity', 'nameplate_capacity'
            ],
            'status': [
                'status', 'Status', 'plant_status', 'current_status',
                'operational_status'
            ],
            'start_year': [
                'start_year', 'start', 'Start Year', 'online_year',
                'commercial_operation', 'operation_start'
            ],
            'retired_year': [
                'retired_year', 'retired', 'Retired Year', 'retirement_year',
                'closure_year', 'shutdown_year'
            ],
            'region': [
                'region', 'Region', 'area', 'geographic_region'
            ],
            'country_area': [
                'country_area', 'country', 'Country', 'Country/Area',
                'nation', 'country_name'
            ],
            'subnational_unit': [
                'subnational_unit', 'state', 'province', 'State/Province',
                'Subnational unit', 'administrative_unit', 'locality'
            ],
            'latitude': [
                'latitude', 'lat', 'Latitude', 'y_coord'
            ],
            'longitude': [
                'longitude', 'lng', 'lon', 'Longitude', 'x_coord'
            ],
            'technology': [
                'technology', 'Technology', 'tech', 'plant_technology'
            ],
            'fuel_type': [
                'fuel_type', 'fuel', 'Fuel', 'primary_fuel'
            ],
            'announced_year': [
                'announced_year', 'announced', 'Announced Year'
            ],
            'construction_start': [
                'construction_start', 'construction', 'Construction Start'
            ],
            'operating_year': [
                'operating_year', 'operating', 'Operating Year'
            ],
            'mothballed_year': [
                'mothballed_year', 'mothballed', 'Mothballed Year'
            ],
            'cancelled_year': [
                'cancelled_year', 'cancelled', 'Cancelled Year'
            ],
            'wiki_url': [
                'wiki_url', 'wiki', 'wikipedia', 'Wiki URL'
            ]
        }
        
        # Map the fields
        for standard_field, possible_names in field_mappings.items():
            for possible_name in possible_names:
                if possible_name in item:
                    value = item[possible_name]
                    # Convert to string and clean
                    if value is not None:
                        record[standard_field] = str(value).strip()
                    break
        
        return record
    
    def try_alternative_methods(self) -> List[Dict]:
        """Try alternative scraping methods if API is not available"""
        logger.info("Trying alternative data extraction methods...")
        
        # Method 1: Look for downloadable data files
        data_urls = [
            "https://globalenergymonitor.org/wp-content/uploads/2023/04/Global-Coal-Plant-Tracker-April-2023.xlsx",
            "https://globalenergymonitor.org/wp-content/uploads/2024/01/Global-Coal-Plant-Tracker-January-2024.xlsx",
            "https://docs.google.com/spreadsheets/d/1W-gobEQugqTR_PP0iczJCrdaR5fWYjIl",
            "https://globalenergymonitor.org/projects/global-coal-plant-tracker/download-data/",
        ]
        
        for url in data_urls:
            try:
                logger.info(f"Trying to download data from: {url}")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    if 'excel' in response.headers.get('content-type', '').lower() or url.endswith('.xlsx'):
                        # Save and process Excel file
                        filename = 'temp_coal_data.xlsx'
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        
                        try:
                            df = pd.read_excel(filename)
                            os.remove(filename)
                            
                            # Convert DataFrame to our format
                            data = []
                            for _, row in df.iterrows():
                                record = self._map_fields(row.to_dict())
                                if record and any(record.values()):
                                    data.append(record)
                            
                            if data:
                                logger.info(f"Successfully extracted {len(data)} records from Excel file")
                                return data
                                
                        except Exception as e:
                            logger.error(f"Error processing Excel file: {e}")
                            if os.path.exists(filename):
                                os.remove(filename)
                    
            except Exception as e:
                logger.debug(f"Failed to download from {url}: {e}")
                continue
        
        # Method 2: Try to find CSV data
        csv_patterns = [
            "/data/coal-plants.csv",
            "/tracker-data/global-coal-plant-tracker.csv",
            "/wp-content/uploads/coal-plant-data.csv"
        ]
        
        for pattern in csv_patterns:
            try:
                url = urljoin(self.base_url, pattern)
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    # Try to parse as CSV
                    from io import StringIO
                    try:
                        df = pd.read_csv(StringIO(response.text))
                        data = []
                        for _, row in df.iterrows():
                            record = self._map_fields(row.to_dict())
                            if record and any(record.values()):
                                data.append(record)
                        
                        if data:
                            logger.info(f"Successfully extracted {len(data)} records from CSV")
                            return data
                            
                    except Exception as e:
                        logger.debug(f"Error parsing CSV from {url}: {e}")
                        
            except Exception as e:
                logger.debug(f"Failed to get CSV from {url}: {e}")
                continue
        
        logger.warning("All alternative methods failed")
        return []
    
    def scrape_all_data(self) -> pd.DataFrame:
        """Main method to scrape all coal plant data"""
        logger.info("Starting Global Coal Plant Tracker data extraction...")
        
        # Step 1: Try to find API endpoint
        api_url = self.get_api_endpoint()
        
        if api_url:
            logger.info(f"Using API endpoint: {api_url}")
            
            # Try direct API scraping
            data = self.scrape_from_api(api_url)
            
            # If no data, try pagination
            if not data:
                data = self.scrape_with_pagination(api_url)
        else:
            data = []
        
        # Step 2: If API fails, try alternative methods
        if not data:
            logger.info("API extraction failed, trying alternative methods...")
            data = self.try_alternative_methods()
        
        # Step 3: Convert to DataFrame
        if data:
            df = pd.DataFrame(data)
            logger.info(f"Successfully extracted {len(df)} coal plant records")
            
            # Clean and validate data
            df = self._clean_dataframe(df)
            
            return df
        else:
            logger.error("Failed to extract any data")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        logger.info("Cleaning and standardizing data...")
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Clean numeric fields
        numeric_fields = ['capacity_mw', 'start_year', 'retired_year', 'announced_year', 
                         'construction_start', 'operating_year', 'mothballed_year', 
                         'cancelled_year', 'latitude', 'longitude']
        
        for field in numeric_fields:
            if field in df.columns:
                # Extract numeric values
                df[field] = pd.to_numeric(df[field].astype(str).str.extract(r'(\d+\.?\d*)')[0], errors='coerce')
        
        # Clean text fields
        text_fields = ['plant_name', 'unit_name', 'owner', 'parent_company', 'status', 
                      'region', 'country_area', 'subnational_unit', 'technology', 'fuel_type']
        
        for field in text_fields:
            if field in df.columns:
                # Clean text
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace(['nan', 'None', ''], pd.NA)
        
        # Sort by country and plant name
        if 'country_area' in df.columns and 'plant_name' in df.columns:
            df = df.sort_values(['country_area', 'plant_name'], na_position='last')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        logger.info(f"Data cleaned. Final dataset has {len(df)} records")
        return df
    
    def save_data(self, df: pd.DataFrame, base_filename: str = "global_coal_plant_tracker_data"):
        """Save data to multiple formats"""
        if df.empty:
            logger.error("No data to save")
            return
        
        try:
            # Save as CSV
            csv_file = f"{base_filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8')
            logger.info(f"Data saved to {csv_file}")
            
            # Save as Excel
            excel_file = f"{base_filename}.xlsx"
            df.to_excel(excel_file, index=False, engine='openpyxl')
            logger.info(f"Data saved to {excel_file}")
            
            # Save summary statistics
            self._save_summary(df, base_filename)
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def _save_summary(self, df: pd.DataFrame, base_filename: str):
        """Save a summary of the data"""
        try:
            summary_file = f"{base_filename}_summary.txt"
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"Global Coal Plant Tracker Data Summary\n")
                f.write(f"Generated on: {pd.Timestamp.now()}\n")
                f.write(f"=" * 50 + "\n\n")
                
                f.write(f"Total Records: {len(df)}\n\n")
                
                # Column info
                f.write("Columns:\n")
                for col in df.columns:
                    non_null = df[col].notna().sum()
                    f.write(f"  {col}: {non_null}/{len(df)} non-null values\n")
                
                f.write("\n")
                
                # Country breakdown
                if 'country_area' in df.columns:
                    f.write("Records by Country:\n")
                    country_counts = df['country_area'].value_counts()
                    for country, count in country_counts.head(20).items():
                        f.write(f"  {country}: {count}\n")
                    if len(country_counts) > 20:
                        f.write(f"  ... and {len(country_counts) - 20} more countries\n")
                
                f.write("\n")
                
                # Status breakdown
                if 'status' in df.columns:
                    f.write("Records by Status:\n")
                    status_counts = df['status'].value_counts()
                    for status, count in status_counts.items():
                        f.write(f"  {status}: {count}\n")
                
                f.write("\n")
                
                # Capacity statistics
                if 'capacity_mw' in df.columns:
                    capacity_stats = df['capacity_mw'].describe()
                    f.write("Capacity Statistics (MW):\n")
                    for stat, value in capacity_stats.items():
                        f.write(f"  {stat}: {value:.2f}\n")
            
            logger.info(f"Summary saved to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving summary: {e}")


def main():
    """Main execution function"""
    try:
        # Create scraper instance
        scraper = GlobalCoalPlantScraper()
        
        # Scrape all data
        df = scraper.scrape_all_data()
        
        if not df.empty:
            # Save the data
            scraper.save_data(df)
            
            print(f"\n✅ Successfully scraped {len(df)} coal plant records!")
            print(f"📁 Data saved to:")
            print(f"   - global_coal_plant_tracker_data.csv")
            print(f"   - global_coal_plant_tracker_data.xlsx")
            print(f"   - global_coal_plant_tracker_data_summary.txt")
            
            # Display sample data
            print(f"\n📊 Sample data:")
            print(df.head().to_string())
            
            # Display columns
            print(f"\n📋 Available columns:")
            for i, col in enumerate(df.columns, 1):
                print(f"   {i:2d}. {col}")
                
        else:
            print("❌ Failed to scrape any data. Please check the logs for details.")
            
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        print(f"❌ An error occurred: {e}")


if __name__ == "__main__":
    main()
