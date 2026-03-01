import requests
import json
import os
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Airtable configuration - from environment
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appjwOgR4HsXeGIda")
OUTPUT_FILE_PATH = '/Users/victorproust/Documents/Work/Priority/EDI/22. MIG_Vendors_Price_Lists.txt'
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('edi_script.log'),
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
class AirtableClient:
    """Client for interacting with Airtable API"""
   
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {config.token}',
            'Content-Type': 'application/json'
        })
        self.session.timeout = 30 # 30 second timeout
   
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
                   
                    if response.status_code == 429: # Rate limited
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
               
                break # Success, exit retry loop
               
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch records from {table_name} after {max_retries} attempts: {e}")
                    raise
                else:
                    wait_time = 2 ** attempt # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time} seconds: {e}")
                    time.sleep(wait_time)
       
        return records
def clean_mojibake(text: str) -> str:
    """Clean up mojibake (encoding errors) in text, replacing common mangled sequences with American English suitable characters."""
    if not text:
        return text
   
    # Define replacements for common mojibake patterns
    replacements = {
        '‚Äô': "'", # Curly apostrophe -> straight apostrophe
        '‚Äôs': "'s", # Possessive form
        '‚Äù': '"', # Right double curly quote -> straight double quote
        '‚Äú': '"', # Left double curly quote -> straight double quote
        '‚Äì': '-', # En dash -> hyphen
        '‚Äî': '--', # Em dash -> double hyphen
        '‚Ä¢': '-', # Bullet -> hyphen
        '‚Ä¶': '...', # Ellipsis
    }
   
    for mangled, clean in replacements.items():
        text = text.replace(mangled, clean)
   
    # Additional cleanup for American English
    text = text.replace('\u2019', "'").replace('\u2018', "'").replace('\u201c', '"').replace('\u201d', '"').replace('\u2014', '-').replace('\u2013', '-')
   
    return text
def format_date(date_str: str) -> str:
    """Format date string to MM/DD/YYYY, preserving leading zeros."""
    if not date_str:
        return ''
    try:
        # Parse various possible date formats from Airtable
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y'):
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%m/%d/%Y')
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {date_str}")
        return date_str # Return original if parsing fails
    except Exception as e:
        logger.error(f"Error formatting date {date_str}: {e}")
        return date_str
def format_unit_price(value: str) -> str:
    """Format unit price to 2 decimal places."""
    if not value or value.strip() == '':
        return ''
    try:
        # Convert to float and round to 2 decimal places
        price_float = float(value)
        rounded_price = round(price_float, 2)
        # Format with exactly 2 decimal places
        formatted_price = f"{rounded_price:.2f}"
        logger.debug(f"Formatted Unit Price: '{value}' -> '{formatted_price}'")
        return formatted_price
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not format Unit Price '{value}' as number: {e}")
        return str(value).strip()
class EDIProcessor:
    """Main processor for EDI data"""
   
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.client = AirtableClient(config)
        self.output_path = Path(config.output_path)
   
    def map_record_to_line(self, fields_mapping: List[str], record: Dict[str, Any]) -> str:
        """Process a record and map fields to a tab-delimited line with validation and cleaning"""
        fields = record.get('fields', {})
        values = []
       
        for field_name in fields_mapping:
            value = fields.get(field_name, '')
           
            # Handle special Airtable field types
            if isinstance(value, dict):
                if 'value' in value:
                    value = value['value'] # Extract the actual value from AI fields
                else:
                    value = json.dumps(value) # Fallback for other dicts
            elif isinstance(value, list):
                value = ', '.join(str(item).strip() for item in value if item)
            else:
                value = str(value).strip() if value is not None else ''
           
            # Clean mojibake and encoding issues
            value = clean_mojibake(value)
           
            # Apply special formatting for specific fields
            if field_name == 'Price List Date':
                value = format_date(value)
            elif field_name == 'Unit Price':
                value = format_unit_price(value)
           
            # Escape tabs and newlines to prevent data corruption
            try:
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            except AttributeError as e:
                logger.error(f"Field '{field_name}' has value: {value} (type: {type(value)}) - Record ID: {record.get('id', 'unknown')}")
                value = str(value) if value is not None else ''
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
           
            values.append(value)
       
        return '\t'.join(values)
   
    def process_table_data(self, table_name: str, view_name: str, fields_mapping: List[str]) -> List[str]:
        """Process a single table and return formatted lines"""
        logger.info(f"Processing {table_name} - {view_name}")
       
        try:
            records = self.client.fetch_records(table_name, view_name)
            lines = []
           
            for record in records:
                try:
                    line = self.map_record_to_line(fields_mapping, record)
                    lines.append(line)
                except Exception as e:
                    logger.error(f"Error processing record {record.get('id', 'unknown')}: {e}")
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
            backup_filename = f'22. MIG_Vendors_Price_Lists_backup_{int(time.time())}.txt'
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
       
        # Define table configurations
        table_configs = [
            {
                'table': 'Vendor Price List',
                'view': 'EDI/Mig Vendor Price List 1',
                'fields': [
                    'Price List Code',
                    'V-Vendor ID (from Vendor)',
                    'Vendor Currency (from Vendor)',
                    'SKU Trim (EDI) (from Products)',
                    'Quantity',
                    'Unit Price',
                    'Price List Date',
                    'Price List Description',
                    'Vendor SKU Trim',
                    'Vendor Name (from Vendor)',
                    'List Currency'
                ]
            }
        ]
       
        # Process each table
        for config in table_configs:
            lines = self.process_table_data(config['table'], config['view'], config['fields'])
            all_lines.extend(lines)
       
        return all_lines
   
    def save_output(self, lines: List[str]) -> None:
        """Save the processed data to the output file"""
        try:
            # Create backup before overwriting
            self.create_backup()
           
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
           
            # Write to file
            with open(self.output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
           
            logger.info(f"Successfully wrote {len(lines)} lines to {self.output_path}")
           
        except Exception as e:
            logger.error(f"Failed to save output: {e}")
            raise
def main():
    """Main function with improved error handling and logging"""
    try:
        # Load configuration from hardcoded values
        config = AirtableConfig.from_hardcoded()
        logger.info("Configuration loaded successfully")
       
        # Initialize processor
        processor = EDIProcessor(config)
       
        # Process all data
        logger.info("Starting EDI data processing...")
        all_lines = processor.process_all_data()
       
        if not all_lines:
            logger.warning("No data was processed")
            return
       
        # Save output
        processor.save_output(all_lines)
       
        logger.info(f"EDI processing completed successfully. Total lines: {len(all_lines)}")
       
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
if __name__ == '__main__':
    main()