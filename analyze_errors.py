#!/usr/bin/env python3
"""
Error Analysis Script for Zoho Contact Sync

This script analyzes the enhanced error logs to identify patterns and provide
actionable insights for resolving Zoho CRM integration issues.
"""

import re
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime
import argparse

def parse_log_file(log_file_path):
    """Parse the enhanced log file and extract error information."""
    
    errors = []
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        current_error = {}
        
        for line in lines:
            line = line.strip()
            
            # Look for error start patterns
            if 'API_EXCEPTION - Failed to create contact' in line or 'RESPONSE_EXCEPTION - Failed to create contact' in line:
                # Save previous error if exists
                if current_error:
                    errors.append(current_error.copy())
                
                # Start new error
                current_error = {
                    'timestamp': extract_timestamp(line),
                    'error_type': 'API_EXCEPTION' if 'API_EXCEPTION' in line else 'RESPONSE_EXCEPTION',
                    'user_email': extract_user_email(line),
                    'details': {}
                }
            
            # Extract error details
            elif current_error and line.startswith('  Account:'):
                account_info = line.replace('  Account: ', '')
                current_error['account_info'] = account_info
                
            elif current_error and line.startswith('  Status:'):
                current_error['status'] = line.replace('  Status: ', '')
                
            elif current_error and line.startswith('  Code:'):
                current_error['code'] = line.replace('  Code: ', '')
                
            elif current_error and line.startswith('  Message:'):
                current_error['message'] = line.replace('  Message: ', '')
                
            elif current_error and line.startswith('  Detail -'):
                detail_match = re.match(r'  Detail - ([^:]+): (.+)', line)
                if detail_match:
                    key, value = detail_match.groups()
                    current_error['details'][key] = value
        
        # Don't forget the last error
        if current_error:
            errors.append(current_error)
            
    except FileNotFoundError:
        print(f"Log file not found: {log_file_path}")
        return []
    except Exception as e:
        print(f"Error reading log file: {e}")
        return []
    
    return errors

def extract_timestamp(line):
    """Extract timestamp from log line."""
    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
    if timestamp_match:
        return timestamp_match.group(1)
    return None

def extract_user_email(line):
    """Extract user email from error line."""
    email_match = re.search(r'for ([^\s]+@[^\s]+)', line)
    if email_match:
        return email_match.group(1)
    return 'Unknown'

def analyze_errors(errors):
    """Analyze errors and provide insights."""
    
    if not errors:
        print("No errors found in the log file.")
        return
    
    print(f"\n=== ERROR ANALYSIS REPORT ===")
    print(f"Total errors found: {len(errors)}")
    print(f"Analysis timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Error type distribution
    error_types = Counter(error['error_type'] for error in errors)
    print(f"\n1. Error Type Distribution:")
    for error_type, count in error_types.items():
        print(f"   {error_type}: {count}")
    
    # Status code distribution
    status_codes = Counter(error.get('status', 'Unknown') for error in errors)
    print(f"\n2. Status Code Distribution:")
    for status, count in status_codes.items():
        print(f"   {status}: {count}")
    
    # Error code distribution
    error_codes = Counter(error.get('code', 'Unknown') for error in errors)
    print(f"\n3. Error Code Distribution:")
    for code, count in error_codes.items():
        print(f"   {code}: {count}")
    
    # Message patterns
    messages = Counter(error.get('message', 'Unknown') for error in errors)
    print(f"\n4. Error Message Patterns:")
    for message, count in messages.items():
        print(f"   {message}: {count}")
    
    # Account-related issues
    accounts_with_issues = defaultdict(int)
    for error in errors:
        if 'account_info' in error:
            account_info = error['account_info']
            # Extract account name (before the ZID part)
            account_name = account_info.split(' (ZID:')[0] if ' (ZID:' in account_info else account_info
            accounts_with_issues[account_name] += 1
    
    print(f"\n5. Accounts with Most Issues:")
    for account, count in sorted(accounts_with_issues.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {account}: {count} errors")
    
    # Specific field issues
    field_issues = defaultdict(int)
    for error in errors:
        for key, value in error.get('details', {}).items():
            field_issues[key] += 1
    
    if field_issues:
        print(f"\n6. Field-Specific Issues:")
        for field, count in sorted(field_issues.items(), key=lambda x: x[1], reverse=True):
            print(f"   {field}: {count}")
    
    # Recommendations
    print(f"\n=== RECOMMENDATIONS ===")
    
    if any(error.get('code') == 'INVALID_DATA' for error in errors):
        print("• INVALID_DATA errors detected:")
        print("  - Check if the account associations are valid")
        print("  - Verify that Account_Name field references exist in Zoho")
        print("  - Ensure user has proper permissions to associate contacts with accounts")
    
    if any('permission' in error.get('message', '').lower() for error in errors):
        print("• Permission-related errors detected:")
        print("  - Review user permissions in Zoho CRM")
        print("  - Check if the integration user has rights to create contacts")
        print("  - Verify field-level permissions for Account_Name association")
    
    if accounts_with_issues:
        print("• Account-specific issues detected:")
        print("  - Review the accounts with the most errors")
        print("  - Check if these accounts exist and are accessible in Zoho")
        print("  - Verify account IDs are correct in the mapping table")
    
    # Generate CSV report
    try:
        df = pd.DataFrame(errors)
        csv_filename = f"./config/error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\n• Detailed error report saved to: {csv_filename}")
    except Exception as e:
        print(f"\n• Could not save CSV report: {e}")

def main():
    parser = argparse.ArgumentParser(description='Analyze Zoho sync error logs')
    parser.add_argument('--log-file', default='./config/zoho_sync_errors.log', 
                        help='Path to the error log file')
    
    args = parser.parse_args()
    
    print("Analyzing Zoho sync errors...")
    errors = parse_log_file(args.log_file)
    analyze_errors(errors)

if __name__ == "__main__":
    main()
