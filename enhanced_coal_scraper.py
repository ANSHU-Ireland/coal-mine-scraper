#!/usr/bin/env python3
"""
Global Coal Plant Tracker Scraper - Enhanced Version
Uses multiple strategies including Selenium for dynamic content
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
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import re

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

class EnhancedCoalPlantScraper:
    """Enhanced scraper with Selenium support for dynamic content"""
    
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
        })
        self.driver = None
        self.data = []
        
    def setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Selenium WebDriver setup successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to setup Selenium: {e}")
            return False
    
    def scrape_with_selenium(self) -> List[Dict]:
        """Use Selenium to scrape dynamic content"""
        if not self.setup_selenium():
            return []
        
        try:
            logger.info("Loading tracker page with Selenium...")
            self.driver.get(self.tracker_url)
            
            # Wait for page to load
            time.sleep(5)
            
            # Look for data tables, iframes, or other content
            data = []
            
            # Method 1: Look for embedded tables
            try:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                logger.info(f"Found {len(tables)} tables on the page")
                
                for table in tables:
                    table_data = self._extract_table_data(table)
                    if table_data:
                        data.extend(table_data)
            except Exception as e:
                logger.debug(f"Error extracting tables: {e}")
            
            # Method 2: Look for iframes with data
            try:
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"Found {len(iframes)} iframes on the page")
                
                for iframe in iframes:
                    iframe_data = self._extract_iframe_data(iframe)
                    if iframe_data:
                        data.extend(iframe_data)
            except Exception as e:
                logger.debug(f"Error extracting iframes: {e}")
            
            # Method 3: Look for JavaScript variables
            try:
                js_data = self._extract_js_data()
                if js_data:
                    data.extend(js_data)
            except Exception as e:
                logger.debug(f"Error extracting JS data: {e}")
            
            # Method 4: Look for AJAX endpoints
            try:
                ajax_data = self._find_ajax_endpoints()
                if ajax_data:
                    data.extend(ajax_data)
            except Exception as e:
                logger.debug(f"Error finding AJAX endpoints: {e}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error in Selenium scraping: {e}")
            return []
        finally:
            if self.driver:
                self.driver.quit()
    
    def _extract_table_data(self, table) -> List[Dict]:
        """Extract data from HTML table"""
        try:
            # Get table HTML and parse with pandas
            table_html = table.get_attribute('outerHTML')
            
            # Use pandas to read the table
            dfs = pd.read_html(table_html)
            
            data = []
            for df in dfs:
                if len(df) > 5:  # Only process tables with substantial data
                    for _, row in df.iterrows():
                        record = self._map_table_row(row.to_dict())
                        if record and any(record.values()):
                            data.append(record)
            
            return data
        except Exception as e:
            logger.debug(f"Error parsing table: {e}")
            return []
    
    def _extract_iframe_data(self, iframe) -> List[Dict]:
        """Extract data from iframe"""
        try:
            # Switch to iframe
            self.driver.switch_to.frame(iframe)
            
            # Look for tables or data in iframe
            data = []
            try:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                for table in tables:
                    table_data = self._extract_table_data(table)
                    if table_data:
                        data.extend(table_data)
            except:
                pass
            
            # Switch back to main content
            self.driver.switch_to.default_content()
            
            return data
        except Exception as e:
            logger.debug(f"Error extracting iframe data: {e}")
            return []
    
    def _extract_js_data(self) -> List[Dict]:
        """Extract data from JavaScript variables"""
        try:
            # Get page source and look for JavaScript data
            page_source = self.driver.page_source
            
            # Look for common patterns
            patterns = [
                r'var\s+coalPlants\s*=\s*(\[.*?\]);',
                r'var\s+data\s*=\s*(\[.*?\]);',
                r'window\.coalData\s*=\s*(\[.*?\]);',
                r'"plants":\s*(\[.*?\])',
                r'"coal_plants":\s*(\[.*?\])',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_source, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if self._validate_coal_data(data):
                            return self._normalize_data(data)
                    except json.JSONDecodeError:
                        continue
            
            return []
        except Exception as e:
            logger.debug(f"Error extracting JS data: {e}")
            return []
    
    def _find_ajax_endpoints(self) -> List[Dict]:
        """Find AJAX endpoints by monitoring network requests"""
        try:
            # Enable network logging
            self.driver.execute_cdp_cmd('Network.enable', {})
            
            # Refresh page to capture network requests
            self.driver.refresh()
            time.sleep(5)
            
            # Get network logs
            logs = self.driver.get_log('performance')
            
            for log in logs:
                message = json.loads(log['message'])
                if message['message']['method'] == 'Network.responseReceived':
                    url = message['message']['params']['response']['url']
                    if any(keyword in url.lower() for keyword in ['coal', 'plant', 'data', 'api']):
                        # Try to fetch this URL
                        try:
                            response = self.session.get(url)
                            if response.status_code == 200:
                                data = response.json()
                                if self._validate_coal_data(data):
                                    return self._normalize_data(data)
                        except:
                            continue
            
            return []
        except Exception as e:
            logger.debug(f"Error finding AJAX endpoints: {e}")
            return []
    
    def _validate_coal_data(self, data) -> bool:
        """Validate if data contains coal plant information"""
        if not data:
            return False
            
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0] if data else {}
        elif isinstance(data, dict):
            first_item = data
        else:
            return False
            
        coal_indicators = [
            'plant', 'unit', 'capacity', 'coal', 'power', 'mw', 'status',
            'country', 'region', 'owner', 'parent', 'start', 'retire'
        ]
        
        if isinstance(first_item, dict):
            keys_lower = [k.lower() for k in first_item.keys()]
            return any(indicator in ' '.join(keys_lower) for indicator in coal_indicators)
        
        return False
    
    def _normalize_data(self, raw_data) -> List[Dict]:
        """Normalize the raw data to standard format"""
        if not raw_data:
            return []
        
        if isinstance(raw_data, dict):
            for key in ['data', 'results', 'items', 'plants', 'records']:
                if key in raw_data and isinstance(raw_data[key], list):
                    raw_data = raw_data[key]
                    break
            else:
                raw_data = [raw_data]
        
        if not isinstance(raw_data, list):
            return []
        
        normalized_data = []
        
        for item in raw_data:
            if not isinstance(item, dict):
                continue
                
            record = self._map_fields(item)
            
            if record and any(record.values()):
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
        
        for standard_field, possible_names in field_mappings.items():
            for possible_name in possible_names:
                if possible_name in item:
                    value = item[possible_name]
                    if value is not None:
                        record[standard_field] = str(value).strip()
                    break
        
        return record
    
    def _map_table_row(self, row_dict: Dict) -> Dict:
        """Map table row to our standard format"""
        return self._map_fields(row_dict)
    
    def try_known_data_sources(self) -> List[Dict]:
        """Try known data sources and repositories"""
        logger.info("Trying known data sources...")
        
        # Known data sources
        data_sources = [
            # GitHub repositories
            "https://raw.githubusercontent.com/GlobalEnergyMonitor/global-coal-plant-tracker/main/data/coal_plants.csv",
            "https://raw.githubusercontent.com/GlobalEnergyMonitor/GCPT/main/Global%20Coal%20Plant%20Tracker.xlsx",
            
            # Direct file links (try various years)
            "https://globalenergymonitor.org/wp-content/uploads/2024/07/Global-Coal-Plant-Tracker-July-2024.xlsx",
            "https://globalenergymonitor.org/wp-content/uploads/2024/04/Global-Coal-Plant-Tracker-April-2024.xlsx",
            "https://globalenergymonitor.org/wp-content/uploads/2024/01/Global-Coal-Plant-Tracker-January-2024.xlsx",
            "https://globalenergymonitor.org/wp-content/uploads/2023/07/Global-Coal-Plant-Tracker-July-2023.xlsx",
            
            # Alternative formats
            "https://globalenergymonitor.org/wp-content/uploads/2024/coal-plant-tracker.csv",
            
            # Try Google Sheets exports
            "https://docs.google.com/spreadsheets/d/1W-gobEQugqTR_PP0iczJCrdaR5fWYjIl/export?format=xlsx",
            "https://docs.google.com/spreadsheets/d/1W-gobEQugqTR_PP0iczJCrdaR5fWYjIl/export?format=csv",
        ]
        
        for url in data_sources:
            try:
                logger.info(f"Trying: {url}")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    # Determine file type and process
                    if url.endswith('.xlsx') or 'excel' in response.headers.get('content-type', '').lower():
                        data = self._process_excel_response(response)
                    elif url.endswith('.csv') or 'csv' in response.headers.get('content-type', '').lower():
                        data = self._process_csv_response(response)
                    else:
                        # Try both
                        try:
                            data = self._process_excel_response(response)
                        except:
                            data = self._process_csv_response(response)
                    
                    if data:
                        logger.info(f"Successfully extracted {len(data)} records from {url}")
                        return data
                        
            except Exception as e:
                logger.debug(f"Failed to get data from {url}: {e}")
                continue
        
        return []
    
    def _process_excel_response(self, response) -> List[Dict]:
        """Process Excel file response"""
        try:
            filename = 'temp_coal_data.xlsx'
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            # Try different sheet names
            xl_file = pd.ExcelFile(filename)
            sheet_names = xl_file.sheet_names
            
            for sheet_name in sheet_names:
                try:
                    df = pd.read_excel(filename, sheet_name=sheet_name)
                    if len(df) > 10:  # Only process sheets with substantial data
                        data = []
                        for _, row in df.iterrows():
                            record = self._map_fields(row.to_dict())
                            if record and any(record.values()):
                                data.append(record)
                        
                        if data and len(data) > 10:
                            os.remove(filename)
                            return data
                except Exception as e:
                    logger.debug(f"Error processing sheet {sheet_name}: {e}")
                    continue
            
            os.remove(filename)
            return []
            
        except Exception as e:
            logger.debug(f"Error processing Excel response: {e}")
            return []
    
    def _process_csv_response(self, response) -> List[Dict]:
        """Process CSV file response"""
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            data = []
            for _, row in df.iterrows():
                record = self._map_fields(row.to_dict())
                if record and any(record.values()):
                    data.append(record)
            
            return data if len(data) > 10 else []
            
        except Exception as e:
            logger.debug(f"Error processing CSV response: {e}")
            return []
    
    def scrape_all_data(self) -> pd.DataFrame:
        """Main method to scrape all coal plant data"""
        logger.info("Starting Enhanced Global Coal Plant Tracker data extraction...")
        
        data = []
        
        # Step 1: Try known data sources first
        logger.info("Step 1: Trying known data sources...")
        data = self.try_known_data_sources()
        
        # Step 2: If no data, try Selenium scraping
        if not data:
            logger.info("Step 2: Trying Selenium scraping...")
            data = self.scrape_with_selenium()
        
        # Step 3: Convert to DataFrame
        if data:
            df = pd.DataFrame(data)
            logger.info(f"Successfully extracted {len(df)} coal plant records")
            df = self._clean_dataframe(df)
            return df
        else:
            logger.error("Failed to extract any data from all methods")
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
                df[field] = pd.to_numeric(df[field].astype(str).str.extract(r'(\d+\.?\d*)')[0], errors='coerce')
        
        # Clean text fields
        text_fields = ['plant_name', 'unit_name', 'owner', 'parent_company', 'status', 
                      'region', 'country_area', 'subnational_unit', 'technology', 'fuel_type']
        
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace(['nan', 'None', ''], pd.NA)
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Sort by country and plant name
        if 'country_area' in df.columns and 'plant_name' in df.columns:
            df = df.sort_values(['country_area', 'plant_name'], na_position='last')
        
        df = df.reset_index(drop=True)
        
        logger.info(f"Data cleaned. Final dataset has {len(df)} records")
        return df
    
    def save_data(self, df: pd.DataFrame, base_filename: str = "global_coal_plant_tracker_data"):
        """Save data to multiple formats"""
        if df.empty:
            logger.error("No data to save")
            return
        
        try:
            csv_file = f"{base_filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8')
            logger.info(f"Data saved to {csv_file}")
            
            excel_file = f"{base_filename}.xlsx"
            df.to_excel(excel_file, index=False, engine='openpyxl')
            logger.info(f"Data saved to {excel_file}")
            
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
                
                f.write("Columns:\n")
                for col in df.columns:
                    non_null = df[col].notna().sum()
                    f.write(f"  {col}: {non_null}/{len(df)} non-null values\n")
                
                f.write("\n")
                
                if 'country_area' in df.columns:
                    f.write("Records by Country:\n")
                    country_counts = df['country_area'].value_counts()
                    for country, count in country_counts.head(20).items():
                        f.write(f"  {country}: {count}\n")
                    if len(country_counts) > 20:
                        f.write(f"  ... and {len(country_counts) - 20} more countries\n")
                
                f.write("\n")
                
                if 'status' in df.columns:
                    f.write("Records by Status:\n")
                    status_counts = df['status'].value_counts()
                    for status, count in status_counts.items():
                        f.write(f"  {status}: {count}\n")
                
                f.write("\n")
                
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
        scraper = EnhancedCoalPlantScraper()
        
        df = scraper.scrape_all_data()
        
        if not df.empty:
            scraper.save_data(df)
            
            print(f"\n✅ Successfully scraped {len(df)} coal plant records!")
            print(f"📁 Data saved to:")
            print(f"   - global_coal_plant_tracker_data.csv")
            print(f"   - global_coal_plant_tracker_data.xlsx")
            print(f"   - global_coal_plant_tracker_data_summary.txt")
            
            print(f"\n📊 Sample data:")
            print(df.head().to_string())
            
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
