import requests
import json
import os
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import sys
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# Airtable configuration - from environment
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appjwOgR4HsXeGIda")
OUTPUT_FILE_PATH = '/Users/victorproust/Documents/Work/Priority/EDI/10. EDI_Products_All.txt'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('edi_products_script.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class AirtableConfig:
    """Configuration class for Airtable settings"""
    token: str
    base_id: str
    output_path: str
    
    @classmethod
    def from_hardcoded(cls) -> 'AirtableConfig':
        """Create config from hardcoded values"""
        return cls(token=AIRTABLE_TOKEN, base_id=AIRTABLE_BASE_ID, output_path=OUTPUT_FILE_PATH)
    
    def validate(self) -> None:
        """Validate configuration values"""
        if not self.token or len(self.token) < 10:
            raise ValueError("Invalid Airtable token")
        if not self.base_id or len(self.base_id) < 10:
            raise ValueError("Invalid Airtable base ID")
        if not self.output_path:
            raise ValueError("Output path cannot be empty")

class FieldMappingValidator:
    """Validator for field mapping configurations"""
    
    @staticmethod
    def validate_table_config(table_config: Dict[str, Any]) -> None:
        """Validate a single table configuration"""
        required_keys = ['table', 'view', 'fields']
        
        # Check required keys
        for key in required_keys:
            if key not in table_config:
                raise ValueError(f"Missing required key '{key}' in table config")
        
        # Validate table name
        if not isinstance(table_config['table'], str) or not table_config['table'].strip():
            raise ValueError("Table name must be a non-empty string")
        
        # Validate view name
        if not isinstance(table_config['view'], str) or not table_config['view'].strip():
            raise ValueError("View name must be a non-empty string")
        
        # Validate fields list
        if not isinstance(table_config['fields'], list) or len(table_config['fields']) == 0:
            raise ValueError("Fields must be a non-empty list")
        
        # Validate each field name
        for i, field in enumerate(table_config['fields']):
            if not isinstance(field, str) or not field.strip():
                raise ValueError(f"Field at index {i} must be a non-empty string")
    
    @staticmethod
    def validate_all_configs(table_configs: List[Dict[str, Any]]) -> None:
        """Validate all table configurations"""
        if not table_configs:
            raise ValueError("Table configurations list cannot be empty")
        
        # Check for duplicate table-view combinations
        seen_combinations = set()
        for config in table_configs:
            combination = (config['table'], config['view'])
            if combination in seen_combinations:
                raise ValueError(f"Duplicate table-view combination: {combination}")
            seen_combinations.add(combination)
            
            # Validate individual config
            FieldMappingValidator.validate_table_config(config)
        
        logger.info(f"Validated {len(table_configs)} table configurations successfully")

class AirtableClient:
    """Client for interacting with Airtable API"""
    
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {config.token}',
            'Content-Type': 'application/json'
        })
        self.session.timeout = 30  # 30 second timeout
    
    def fetch_records(self, table_name: str, view_name: str, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Fetch all records from a table and view with retry logic and pagination"""
        records = []
        offset = None
        base_url = f'https://api.airtable.com/v0/{self.config.base_id}/{requests.utils.quote(table_name)}'
        
        for attempt in range(max_retries):
            try:
                while True:
                    params = {'view': view_name}
                    if offset:
                        params['offset'] = offset
                    
                    logger.info(f"Fetching records from {table_name} - {view_name} (offset: {offset or 'start'})")
                    response = self.session.get(base_url, params=params)
                    
                    if response.status_code == 429:  # Rate limited
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                        time.sleep(retry_after)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    new_records = data.get('records', [])
                    records.extend(new_records)
                    logger.info(f"Retrieved {len(new_records)} records from {table_name}")
                    
                    offset = data.get('offset')
                    if not offset:
                        break
                    
                    # Small delay to be respectful to the API
                    time.sleep(0.1)
                
                break  # Success, exit retry loop
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch records from {table_name} after {max_retries} attempts: {e}")
                    raise
                else:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time} seconds: {e}")
                    time.sleep(wait_time)
        
        return records

class EDIProcessor:
    """Main processor for EDI data"""
    
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.client = AirtableClient(config)
        self.output_path = Path(config.output_path)
    
    def format_numeric_value(self, value: str, field_name: str) -> str:
        """Format numeric values to exactly two decimal places for price and cost fields"""
        if not isinstance(value, str) or not value.strip():
            return value
        
        # Only process fields that are price or cost related
        price_cost_fields = ['price', 'cost', 'sale', 'base price', 'standard cost', 'lvl', 'level']
        is_price_cost_field = any(term in field_name.lower() for term in price_cost_fields)
        
        if not is_price_cost_field:
            return value  # Return unchanged for non-price/cost fields
        
        try:
            # Only try to convert to float and format to 2 decimal places
            numeric_value = float(value)
            formatted_value = f"{numeric_value:.2f}"
            return formatted_value
        except (ValueError, TypeError):
            # If conversion fails, return the original value unchanged
            return value
    
    def map_record_to_line(self, fields_mapping: List[str], record: Dict[str, Any]) -> List[str]:
        """Process a record and map fields to tab-delimited lines with validation. Returns a list of lines for cases with multiples."""
        fields = record.get('fields', {})
        values = []
        
        for field_name in fields_mapping:
            value = fields.get(field_name, '')
            
            # Handle special Airtable field types
            if isinstance(value, dict):
                if 'value' in value:
                    value = value['value']  # Extract the actual value from AI fields
                else:
                    value = json.dumps(value)  # Fallback for other dicts
            elif isinstance(value, list):
                # Handle lookup or linked fields (list of values)
                sku_values = []
                for item in value:
                    if isinstance(item, str):
                        sku_values.append(item.strip())
                    elif isinstance(item, dict):
                        # If expanded, extract relevant value
                        sku_field = item.get('SKU Trim (EDI)') or item.get('SKU') or item.get('Product Code') or json.dumps(item)
                        sku_values.append(str(sku_field))
                    else:
                        sku_values.append(str(item))
                
                value = ', '.join(filter(None, sku_values))  # Join non-empty values
            else:
                value = str(value).strip() if value is not None else ''
            
            # Format numeric values to exactly two decimal places (only for price/cost fields)
            value = self.format_numeric_value(value, field_name)
            
            # Escape tabs and newlines to prevent data corruption
            try:
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            except AttributeError:
                # Log which field is causing the issue
                logger.error(f"Field '{field_name}' has value: {value} (type: {type(value)}) - Record ID: {record.get('id', 'unknown')}")
                # Convert to string safely
                value = str(value) if value is not None else ''
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            
            values.append(value)
        
        # Skip the line if Column B (index 1) is empty after processing (no valid SKU found)
        if len(values) > 1 and not values[1].strip():
            logger.warning(f"Skipping record {record.get('id', 'unknown')} because no valid SKU found in Column B")
            return []
        
        # Default: single line
        return ['\t'.join(values)]
    
    def process_table_data(self, table_name: str, view_name: str, fields_mapping: List[str]) -> List[str]:
        """Process a single table and return formatted lines"""
        logger.info(f"Processing {table_name} - {view_name}")
        
        try:
            records = self.client.fetch_records(table_name, view_name)
            lines = []
            
            # Add progress bar for record processing
            with tqdm(total=len(records), desc=f"Processing {table_name}", unit="records") as pbar:
                for record in records:
                    try:
                        record_lines = self.map_record_to_line(fields_mapping, record)
                        lines.extend(record_lines)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing record {record.get('id', 'unknown')}: {e}")
                        pbar.update(1)
                        continue
            
            logger.info(f"Successfully processed {len(lines)} lines from {table_name}")
            return lines
            
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {e}")
            return []
    
    def create_backup(self) -> None:
        """Create a backup of the existing output file if it exists"""
        if self.output_path.exists():
            # Create backup directory path
            backup_dir = Path('/Users/victorproust/Documents/Work/Priority/EDI/Backup')
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Create backup filename with timestamp
            backup_filename = f'product_all_backup_{int(time.time())}.txt'
            backup_path = backup_dir / backup_filename
            
            try:
                import shutil
                shutil.copy2(self.output_path, backup_path)
                logger.info(f"Created backup: {backup_path}")
            except Exception as e:
                logger.warning(f"Failed to create backup: {e}")
    
    def process_all_data(self) -> List[str]:
        """Process all EDI data from all tables"""
        all_lines = []
        
        # Define table configurations (updated EDI 5.1, 5.2, 5.3 with additional fields)
        table_configs = [
            {
                'table': 'Products',
                'view': 'EDI Parts 1 - Part Catalogue',
                'fields': [
                    'EDI 1', 'SKU Trim (EDI)', 'Brand', 'Brand + Product Title + Net Weight + Case Pack',
                    'Product Net Weight Input', 'Product Net Weight Unit Input', 'Case Pack',
                    'Buy_Sell Unit (Priority)', 'Base Price', 'Base Price Currency', 'Priority Status',
                    'Inventory Status', 'Catalog Status', 'V-Vendor ID (from Preferred Vendor)', 'Standard Cost',
                    'Kelsey_Categories', 'Kelsey_Subcategories', 'Perishable', 'Retail',
                    'Feature_Individual Portions', 'Staff Pick', 'Storage', 'Availability Priority Output',
                    'Direct Import', 'LVL 2 SALE PRICE (from Price Import)', 'Conversion Ratio','Family (Number from Product Type)','Type (P/R/O)','Vendor SKU Trim','Allocate Inventory'
                ]
            },
            {
                'table': 'Shelf Lives',
                'view': 'EDI Parts 2 - Shelf Lives',
                'fields': [
                    'EDI 2', 'SKU Trim (EDI) (from Products)', 'Type Label (Custom)', 'Shelf Life Input', 'Shelf Life Unit Input'
                ]
            },
            {
                'table': 'Products',
                'view': 'EDI Parts 3 - Allergens & Features',
                'fields': [
                    'EDI 3', 'SKU Trim (EDI)', 'Allergen_Allergen Present', 'Allergen_Eggs',
                    'Allergen_Dairy', 'Allergen_Fish', 'Allergen_Peanut', 'Allergen_Sesame',
                    'Allergen_Shellfish', 'Allergen_Soybean', 'Allergen_Tree Nuts', 'Allergen_Wheat',
                    'Feature_Feature Present', 'Feature_Gluten Free', 'Feature_Organic', 'Feature_Kosher',
                    'Feature_Vegan', 'Feature_Halal', 'Feature_Non GMO', 'Feature_Identity Protected',
                    'GFSI Certified', 'Glass Packaging', 'Prop. 65 Warning', 'Calif. Ass. Bill 418',
                    'Traceability Type Output'
                ]
            },
            {
                'table': 'Products',
                'view': 'EDI Parts 5.1 - Price Lvl 1 (Base)',
                'fields': [
                    'EDI 5', 'SKU Trim (EDI)', 'Lvl 1 Price List Code',
                    'LVL 1 SALE PRICE (from Price Import)', 'EDI $', 'EDI Price Quantity',
                    'Buy_Sell Unit (Priority)'
                ]
            },
            {
                'table': 'Products',
                'view': 'EDI Parts 5.2 - Price Lvl 2 (Whole)',
                'fields': [
                    'EDI 5', 'SKU Trim (EDI)', 'Lvl 2 Price List Code',
                    'LVL 2 SALE PRICE (from Price Import)', 'EDI $', 'EDI Price Quantity',
                    'Buy_Sell Unit (Priority)'
                ]
            },
            {
                'table': 'Products',
                'view': 'EDI Parts 5.3 - Price Lvl 3',
                'fields': [
                    'EDI 5', 'SKU Trim (EDI)', 'Lvl 3 Price List Code',
                    'LVL 3 SALE PRICE (from Price Import)', 'EDI $', 'EDI Price Quantity',
                    'Buy_Sell Unit (Priority)'
                ]
            },
            {
                'table': 'Products',
                'view': 'EDI Parts 6 - Bins',
                'fields': [
                    'EDI 6', 'SKU Trim (EDI)', 'EDI Main', 'Simplified Bin Location (from Bin # Priority)'
                ]
            }
        ]
        
        # Validate all table configurations before processing
        try:
            FieldMappingValidator.validate_all_configs(table_configs)
        except ValueError as e:
            logger.error(f"Field mapping validation failed: {e}")
            raise
        
        # Process each table with overall progress indicator
        with tqdm(total=len(table_configs), desc="Processing tables", unit="table") as table_pbar:
            for config in table_configs:
                lines = self.process_table_data(config['table'], config['view'], config['fields'])
                all_lines.extend(lines)
                table_pbar.update(1)
                table_pbar.set_postfix({
                    'lines_processed': len(all_lines),
                    'current_table': config['table']
                })
        
        # Sort by column B (index 1) first, then by column A (index 0)
        logger.info("Sorting output by column B, then by column A...")
        all_lines.sort(key=lambda line: (line.split('\t')[1] if len(line.split('\t')) > 1 else '', line.split('\t')[0] if len(line.split('\t')) > 0 else ''))
        
        return all_lines
    
    def save_output(self, lines: List[str]) -> None:
        """Save the processed data to the output file"""
        try:
            # Create backup before overwriting
            self.create_backup()
            
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to file with progress indicator
            logger.info(f"Writing {len(lines)} lines to output file...")
            with tqdm(total=len(lines), desc="Writing to file", unit="lines") as pbar:
                with open(self.output_path, 'w', encoding='utf-8') as f:
                    for line in lines:
                        f.write(line + '\n')
                        pbar.update(1)
            
            logger.info(f"Successfully wrote {len(lines)} lines to {self.output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save output: {e}")
            raise

def main():
    """Main function with improved error handling and logging"""
    try:
        # Load configuration from hardcoded values
        config = AirtableConfig.from_hardcoded()
        
        # Validate configuration
        try:
            config.validate()
            logger.info("Configuration validation passed")
        except ValueError as e:
            logger.error(f"Configuration validation failed: {e}")
            sys.exit(1)
        
        logger.info("Configuration loaded successfully")
        
        # Initialize processor
        processor = EDIProcessor(config)
        
        # Process all data
        logger.info("Starting EDI products data processing...")
        all_lines = processor.process_all_data()
        
        if not all_lines:
            logger.warning("No data was processed")
            return
        
        # Save output
        processor.save_output(all_lines)
        
        logger.info(f"EDI products processing completed successfully. Total lines: {len(all_lines)}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()