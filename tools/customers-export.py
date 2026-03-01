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
OUTPUT_FILE_PATH = '/Users/victorproust/Documents/Work/Priority/EDI/30. EDI_Customers_All.txt'

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

def abbreviate_day(day: str) -> str:
    """Abbreviate day name to 3-letter code"""
    day_lower = day.strip().lower()
    mapping = {
        'monday': 'Mon', 'tuesday': 'Tue', 'wednesday': 'Wed',
        'thursday': 'Thu', 'friday': 'Fri', 'saturday': 'Sat', 'sunday': 'Sun',
        'mon': 'Mon', 'tue': 'Tue', 'wed': 'Wed', 'thu': 'Thu',
        'fri': 'Fri', 'sat': 'Sat', 'sun': 'Sun',
    }
    return mapping.get(day_lower, day[:3].capitalize() if day else '')

def format_time(time_str: str) -> str:
    """Convert time from 12-hour AM/PM to 24-hour format with leading zero"""
    if not time_str:
        return ''
    parts = time_str.split(' ')
    if len(parts) != 2:
        return time_str
    time_part, ampm = parts
    hh_mm = time_part.split(':')
    if len(hh_mm) != 2:
        return time_str
    hh, mm = hh_mm
    try:
        hh_int = int(hh)
        ampm = ampm.upper()
        if ampm == 'PM' and hh_int != 12:
            hh_int += 12
        elif ampm == 'AM' and hh_int == 12:
            hh_int = 0
        hh = f"{hh_int:02d}"
    except ValueError:
        return time_str
    return f"{hh}:{mm}"

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
        self.session.timeout = 30

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
                    if response.status_code == 429:
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
                    time.sleep(0.1)
                break
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch records from {table_name} after {max_retries} attempts: {e}")
                    raise
                else:
                    wait_time = 2 ** attempt
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time} seconds: {e}")
                    time.sleep(wait_time)
        return records

class EDIProcessor:
    """Main processor for EDI data"""
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.client = AirtableClient(config)
        self.output_path = Path(config.output_path)

    def map_record_to_line(self, fields_mapping: List[str], record: Dict[str, Any]) -> str:
        """Process a record and map fields to a tab-delimited line with validation"""
        fields = record.get('fields', {})
        values = []
        rec_id = record.get('id', 'unknown')

        # DEBUG: For EDI 7, print raw fields
        if 'EDI_7' in fields_mapping[0]:  # First field is EDI_7 for this table
            print("\n=== DEBUG: Raw Airtable Fields for EDI 7 Record ===")
            for field_name in fields_mapping:
                value = fields.get(field_name, '*** MISSING FIELD ***')
                print(f"  {field_name}: '{value}' (type: {type(value)})")

        for field_name in fields_mapping:
            value = fields.get(field_name, '')

            # Handle special Airtable field types
            if isinstance(value, dict):
                if 'value' in value:
                    value = value['value']
                else:
                    value = json.dumps(value)
            elif isinstance(value, list):
                value = ', '.join(str(item).strip() for item in value if item)
            else:
                value = str(value).strip() if value is not None else ''

            # Escape tabs and newlines to prevent data corruption
            try:
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            except AttributeError:
                logger.warning(f"Unexpected None value in field '{field_name}' - Record ID: {rec_id}")
                value = ''

            if value == '' or value is None:
                logger.warning(f"Field '{field_name}' is empty - Record ID: {rec_id}")

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
            backup_filename = f'30. EDI_Customers_All_backup_{int(time.time())}.txt'
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

        table_configs = [
            {
                'table': 'Customers',
                'view': 'EDI Cust. 1',
                'fields': [
                    'EDI_1', 'Priority Cust. ID', 'Business Name Output', 'Sales Rep Number',
                    'Cust Group Code', 'Website Output', 'Zone Code', 'Billing Address Line 1 Output',
                    'Billing Address_Line 2 Output', 'Billing Address_City Output', 'Full State Name',
                    'Billing Address_Zip Code', 'Payment Terms Code', 'Credit Limit', 'Payment Method (1)',
                    'Are you part of Hilton? (2)', 'Hilton Inncode (3)', 'Rekki Output (4)',
                    'Carrier Account Number (5)', 'Curr', 'Country', 'Tax Code', 'Dist. Route Code', 'Approval_Before_Charging','Shipment Code','Priority Customer Status','Billing_Legal Name Output'
                ]
            },
            {
                'table': 'Customers',
                'view': 'EDI Cust. 2 - Price List',
                'fields': ['EDI 2', 'Priority Cust. ID', 'Price List Code']
            },
            {
                'table': 'Special Cust. Prices',
                'view': 'EDI Cust. 3 - Special Price',
                'fields': ['EDI 3', 'Cust. IDs', 'SKU', 'Formatted Start Date', 'Expiration Date', 'Special Price']
            },
            {
                'table': 'Customers',
                'view': 'EDI Cust. 4 - Shipment Remarks',
                'fields': ['EDI_4', 'Priority Cust. ID', 'Cleaned Delivery Instructions']
            },
            {
                'table': 'Customers',
                'view': 'EDI Cust. 5 - Delivery Days',
                'fields': ['EDI_5', 'Priority Cust. ID', 'Days of Business', 'Deliver After', 'Deliver Before']
            },
            {
                'table': 'Customer Contacts 2025',
                'view': 'EDI Cust. 6 - Cust. Contacts',
                'fields': [
                    'EDI 6', 'Priority Cust. ID (from Customers)', 'Clean First Name', 'Clean Last Name',
                    'Clean Phone Number', 'Clean Cell Phone', 'Email_ID', 'Consent to Receive Emails Output',
                    'Clean Position', 'Status', 'Linkedin', 'Sign Up Priority', 'Clean Full Name'
                ]
            },
            {
                'table': 'Customer Sites',
                'view': 'EDI Cust. 7 - Sites',
                'fields': [
                    'EDI_7', 'Priority Cust. ID', 'Site Id', 'Ship To Name Output',
                    'Main Output', 'Address Line 1 Output', 'Address Line 2 Output', 'Address Remarks Output', 'City Output', 'State', 'Zip', 'EDI_USA', 'Primary receiver (from Customers)', 'Phone Output', 'Zone Code', 'Dist. Route Code', 'EDI_Carrier Code', 'Carrier Account Number (5)'
                ]
            },
            {
                'table': 'Customers',
                'view': 'EDI Cust. 8 - Internal Remarks',
                'fields': ['EDI_8', 'Priority Cust. ID', 'Billing_Instructions_Output']
            }
        ]

        for config in table_configs:
            lines = self.process_table_data(config['table'], config['view'], config['fields'])
            if config['view'] == 'EDI Cust. 5 - Delivery Days':
                transformed_lines = []
                for line in lines:
                    parts = line.split('\t')
                    if len(parts) >= 5:
                        edi_5 = parts[0]
                        cust_id = parts[1]
                        days_str = parts[2]
                        after = format_time(parts[3])
                        before = format_time(parts[4])
                        days = [d.strip() for d in days_str.split(',') if d.strip()]
                        for day in days:
                            abbrev = abbreviate_day(day)
                            new_line = '\t'.join([edi_5, cust_id, abbrev, after, before])
                            transformed_lines.append(new_line)
                all_lines.extend(transformed_lines)
            else:
                all_lines.extend(lines)

        logger.info("Sorting output by column B, then by column A...")
        all_lines.sort(key=lambda line: (
            line.split('\t')[1] if len(line.split('\t')) > 1 else '',
            line.split('\t')[0] if len(line.split('\t')) > 0 else ''
        ))
        return all_lines

    def save_output(self, lines: List[str]) -> None:
        """Save the processed data to the output file"""
        try:
            self.create_backup()
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
            logger.info(f"Successfully wrote {len(lines)} lines to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to save output: {e}")
            raise

def main():
    """Main function with improved error handling and logging"""
    try:
        config = AirtableConfig.from_hardcoded()
        logger.info("Configuration loaded successfully")
        processor = EDIProcessor(config)
        logger.info("Starting EDI data processing...")
        all_lines = processor.process_all_data()
        if not all_lines:
            logger.warning("No data was processed")
            return
        processor.save_output(all_lines)
        logger.info(f"EDI processing completed successfully. Total lines: {len(all_lines)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()