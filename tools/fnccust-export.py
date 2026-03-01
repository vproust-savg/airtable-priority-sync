import requests
import json
import os
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import sys

from dotenv import load_dotenv

load_dotenv()

# Airtable configuration - from environment
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appjwOgR4HsXeGIda")
OUTPUT_FILE_PATH = '/Users/victorproust/Documents/Work/Priority/EDI/31. EDI_Customers_Financial_Parameters.txt'

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

def clean_mojibake(text: str) -> str:
    """Clean up mojibake (encoding errors) in text, replacing common mangled sequences with American English suitable characters.
    Enhanced to remove common invisible/hidden characters that could affect EDI import recognition."""
    if not text:
        return text
    
    # Define replacements for common mojibake patterns
    replacements = {
        '‚Äô': "'",      # Curly apostrophe -> straight apostrophe
        '‚Äôs': "'s",    # Possessive form
        '‚Äù': '"',      # Right double curly quote -> straight double quote
        '‚Äú': '"',      # Left double curly quote -> straight double quote
        '‚Äì': '-',      # En dash -> hyphen
        '‚Äî': '--',     # Em dash -> double hyphen
        '‚Ä¢': '-',      # Bullet -> hyphen
        '‚Ä¶': '...',    # Ellipsis
    }
    
    for mangled, clean in replacements.items():
        text = text.replace(mangled, clean)
    
    # Additional cleanup for American English and hidden chars
    text = text.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"').replace('—', '-').replace('–', '-')
    
    # Remove common invisible/zero-width characters that might not be caught by strip()
    invisible_chars = [
        '\u200b',  # Zero-width space
        '\u200c',  # Zero-width non-joiner
        '\u200d',  # Zero-width joiner
        '\u2060',  # Word joiner
        '\ufeff',  # Byte order mark
        '\u00ad',  # Soft hyphen
        '\u180e',  # Mongolian vowel separator
    ]
    for char in invisible_chars:
        text = text.replace(char, '')
    
    return text.strip()  # Final aggressive strip to remove all whitespace

class EDIProcessor:
    """Main processor for EDI data"""
    
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.client = AirtableClient(config)
        self.output_path = Path(config.output_path)
        self.diagnostic_count = 0  # Counter for logging EDI_Y diagnostics (limit to first 5 records)
    
    def map_record_to_line(self, fields_mapping: List[str], record: Dict[str, Any]) -> str:
        """Process a record and map fields to a tab-delimited line with validation and cleaning"""
        fields = record.get('fields', {})
        values = []
        fields_requiring_leading_zeros = {'Cust Group Code', 'Tax Code', 'Payment Terms Code', 'Finance Charge Code'}
        
        for field_name in fields_mapping:
            value = fields.get(field_name, '')
            
            # Handle special Airtable field types
            if isinstance(value, dict):
                if 'value' in value:
                    value = value['value']  # Extract the actual value from AI fields
                else:
                    value = json.dumps(value)  # Fallback for other dicts
            elif isinstance(value, list):
                value = ', '.join(str(item).strip() for item in value if item)
            else:
                value = str(value).strip() if value is not None else ''
            
            # Preserve leading zeros for specified fields (zfill maintains original length to keep zeros)
            if field_name in fields_requiring_leading_zeros and value:
                original_len = len(value)
                value = str(value).zfill(original_len)
            
            # Special handling for EDI_Y field: normalize to 'Y' and log diagnostics
            if field_name == 'EDI_Y':
                # Diagnostic logging for first 5 records to check for hidden chars
                if self.diagnostic_count < 5:
                    logger.info(f"EDI_Y diagnostic (record {record.get('id', 'unknown')}): raw='{value}', repr={repr(value)}, len={len(value)}, chars={[f'\\u{ord(c):04x}' for c in value]}")
                    self.diagnostic_count += 1
                
                # Force to 'Y' for yes (case-insensitive 'y' or 'Y'), empty otherwise
                cleaned = clean_mojibake(value)
                if cleaned.upper() == 'Y':
                    value = 'Y'  # Output 'Y' for Priority compatibility
                else:
                    value = ''  # Or 'N' if Priority requires it for false, but assuming empty is fine
                logger.debug(f"EDI_Y processed: '{value}' (original cleaned: '{cleaned}')")
            else:
                # Clean other fields, including Curr and Pay to Bank Account, to remove whitespace and hidden chars
                value = clean_mojibake(value)
            
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
            backup_dir = Path('/Users/victorproust/Documents/Work/Priority/EDI/Backup')
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            backup_filename = f'31. EDI_Customers_Financial_Parameters_backup_{int(time.time())}.txt'
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
        
        # Define table configuration for Customers
        table_configs = [
            {
                'table': 'Customers',
                'view': 'EDI Cust. Financials 1',
                'fields': [
                    'EDI_1', 'Priority Cust. ID', 'Billing Priority Cust. ID (from Billing Cust. Number)',
                    'Cust Group Code', 'Priority Customer Status', 'Financial Parameters Assigned',
                    'Financial Account No', 'Tax Code', 'Invoice Type', 'Payment Terms Code',
                    'Finance Charge Code', 'Curr', 'EDI_Y', 'Pay to Bank Account'
                ]
            }
        ]
        
        # Process each table
        for config in table_configs:
            lines = self.process_table_data(config['table'], config['view'], config['fields'])
            all_lines.extend(lines)
        
        # No sorting; return lines in Airtable retrieval order
        logger.info("Processing complete; no sorting applied.")
        return all_lines
    
    def save_output(self, lines: List[str]) -> None:
        """Save the processed data to the output file"""
        try:
            # Create backup before overwriting
            self.create_backup()
            
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to file with explicit UTF-8 encoding (no BOM)
            with open(self.output_path, 'w', encoding='utf-8', newline='') as f:
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
        logger.info("Check edi_script.log for EDI_Y diagnostics to confirm no hidden characters.")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()