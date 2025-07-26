# Global Coal Plant Tracker Scraper

A comprehensive Python scraper for extracting coal plant data from the Global Energy Monitor's Coal Plant Tracker.

## Overview

This scraper extracts detailed information about coal plants worldwide, including:

- **Plant Information**: Plant name, unit name, plant/unit name
- **Ownership**: Owner, parent company
- **Technical Details**: Capacity (MW), technology, fuel type
- **Status**: Current operational status
- **Timeline**: Start year, retired year, announced year, construction start, operating year, mothballed year, cancelled year
- **Location**: Country/area, region, subnational unit, latitude, longitude
- **Additional**: Wiki URL and other metadata

## Features

- ✅ **Multiple Data Sources**: Tries API endpoints, downloadable files, and embedded data
- ✅ **Comprehensive Field Mapping**: Handles various field name variations
- ✅ **Data Validation**: Cleans and validates extracted data
- ✅ **Multiple Output Formats**: CSV, Excel, and summary reports
- ✅ **Robust Error Handling**: Continues operation if some methods fail
- ✅ **Progress Tracking**: Detailed logging and progress information
- ✅ **Pagination Support**: Handles large datasets with pagination

## Installation

1. **Clone or download this repository**

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

Run the scraper to extract all coal plant data:

```bash
python coal_plant_scraper.py
```

### Output Files

The scraper generates the following files:

1. **`global_coal_plant_tracker_data.csv`** - Main dataset in CSV format
2. **`global_coal_plant_tracker_data.xlsx`** - Main dataset in Excel format  
3. **`global_coal_plant_tracker_data_summary.txt`** - Summary statistics and overview
4. **`coal_scraper.log`** - Detailed execution log

### Data Structure

The extracted data includes these columns:

| Column | Description |
|--------|-------------|
| `plant_name` | Name of the coal plant |
| `unit_name` | Name of the specific unit |
| `plant_unit_name` | Combined plant/unit identifier |
| `owner` | Operating company/owner |
| `parent_company` | Parent or holding company |
| `capacity_mw` | Power capacity in megawatts |
| `status` | Current operational status |
| `start_year` | Year the plant started operation |
| `retired_year` | Year the plant was retired (if applicable) |
| `region` | Geographic region |
| `country_area` | Country or area |
| `subnational_unit` | State, province, or other subnational division |
| `latitude` | Geographic latitude |
| `longitude` | Geographic longitude |
| `technology` | Plant technology type |
| `fuel_type` | Primary fuel type |
| `announced_year` | Year the plant was announced |
| `construction_start` | Year construction began |
| `operating_year` | Year the plant became operational |
| `mothballed_year` | Year the plant was mothballed (if applicable) |
| `cancelled_year` | Year the plant was cancelled (if applicable) |
| `wiki_url` | Wikipedia URL (if available) |

## How It Works

### Data Extraction Strategy

The scraper uses a multi-layered approach:

1. **API Discovery**: Searches for REST API endpoints
2. **Direct File Download**: Attempts to download Excel/CSV files
3. **Embedded Data Extraction**: Extracts JSON data from web pages
4. **Pagination Handling**: Manages large datasets across multiple pages

### Field Mapping

The scraper intelligently maps various field names to a standardized format:

- Handles different naming conventions (camelCase, snake_case, Title Case)
- Maps synonymous fields (e.g., "capacity", "mw", "power_mw" → "capacity_mw")
- Preserves all available data while standardizing the structure

### Data Cleaning

- Removes empty records and invalid data
- Standardizes numeric fields (capacity, years, coordinates)
- Cleans text fields (removes extra whitespace, handles null values)
- Sorts data by country and plant name

## Troubleshooting

### Common Issues

1. **No data extracted**: 
   - Check your internet connection
   - The website structure may have changed
   - Check the log file for detailed error messages

2. **Partial data**: 
   - Some extraction methods may fail while others succeed
   - Check the summary file to see what data was captured

3. **Installation issues**:
   - Ensure you have Python 3.7+ installed
   - Use a virtual environment to avoid dependency conflicts

### Logs

Check `coal_scraper.log` for detailed information about:
- Which extraction methods were tried
- Success/failure of each attempt
- Data validation results
- Any errors encountered

## Data Source

This scraper extracts data from the [Global Energy Monitor's Global Coal Plant Tracker](https://globalenergymonitor.org/projects/global-coal-plant-tracker/tracker/).

**Note**: Please respect the data source's terms of use and rate limits. The scraper includes delays between requests to be respectful of the server.

## License

This project is for educational and research purposes. Please ensure compliance with the data source's terms of service.

## Contributing

Feel free to submit issues or pull requests to improve the scraper's functionality or add new features.
