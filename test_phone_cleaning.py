#!/usr/bin/env python3
"""
Test script to validate phone number cleaning functionality.
This script helps you test the phone cleaning functions before running the full sync.
"""

import pandas as pd
import re

def clean_phone_number(phone_str):
    """
    Clean phone number by removing hidden control characters and invalid characters.
    Returns a properly formatted phone number or None if invalid.
    """
    if phone_str is None or pd.isna(phone_str):
        return None
    
    # Convert to string if not already
    phone_str = str(phone_str)
    
    # Log the original value for debugging
    print(f"Original phone value: {repr(phone_str)}")
    
    # Remove all control characters (ASCII 0-31 and 127)
    # This includes hidden characters like \x00, \x01, etc.
    cleaned = ''.join(char for char in phone_str if ord(char) >= 32 and ord(char) != 127)
    
    # Remove any remaining non-printable characters
    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
    
    # Remove extra whitespace
    cleaned = cleaned.strip()
    
    # If empty after cleaning, return None
    if not cleaned or cleaned.lower() in ['null', 'none', 'n/a', '']:
        return None
    
    # Remove any non-digit, non-space, non-dash, non-parentheses, non-plus characters
    # Keep only valid phone number characters
    cleaned = re.sub(r'[^\d\s\-\(\)\+\.]', '', cleaned)
    
    # Remove excessive spaces and normalize
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # If still empty or too short, return None
    if not cleaned or len(re.sub(r'[^\d]', '', cleaned)) < 7:
        return None
    
    print(f"Cleaned phone value: {repr(cleaned)}")
    return cleaned

def test_phone_cleaning():
    """Test the phone cleaning function with various inputs."""
    
    test_cases = [
        # Normal cases
        "555-123-4567",
        "(555) 123-4567",
        "+1 555 123 4567",
        "5551234567",
        
        # Cases with control characters (simulate what might be in your data)
        "555\x00123\x014567",  # NULL and SOH characters
        "555\t123\n4567",      # Tab and newline
        "555\r123 4567",       # Carriage return
        "\x02555-123-4567\x03", # STX and ETX characters
        
        # Edge cases
        None,
        "",
        "null",
        "N/A",
        "555",  # Too short
        "abc-def-ghij",  # No digits
        "555-123-4567-extra-text",  # Extra text that should be cleaned
        
        # Cases with multiple spaces and formatting issues
        "  555   123    4567  ",
        "555..123..4567",
        "555--123--4567",
    ]
    
    print("=== PHONE NUMBER CLEANING TEST ===\n")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"Test {i}:")
        result = clean_phone_number(test_case)
        print(f"Result: {repr(result)}")
        print("-" * 40)

def test_with_sample_data():
    """Test with a sample dataframe similar to your actual data."""
    
    print("\n=== SAMPLE DATAFRAME TEST ===\n")
    
    # Create sample data with potential issues
    sample_data = {
        'UserEmail': ['test1@example.com', 'test2@example.com', 'test3@example.com'],
        'UserCellPhone': [
            '555-123-4567',  # Normal
            '555\x00123\x014567',  # With control characters
            '\t555 123 4567\r\n'  # With tab, CR, LF
        ],
        'FirstName': ['John', 'Jane', 'Bob'],
        'LastName': ['Doe', 'Smith', 'Johnson']
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Original data:")
    for i, row in df.iterrows():
        print(f"Row {i}: Email={row['UserEmail']}, Phone={repr(row['UserCellPhone'])}")
    
    print("\nCleaned data:")
    for i, row in df.iterrows():
        cleaned_phone = clean_phone_number(row['UserCellPhone'])
        print(f"Row {i}: Email={row['UserEmail']}, Cleaned Phone={repr(cleaned_phone)}")

if __name__ == "__main__":
    test_phone_cleaning()
    test_with_sample_data()
    
    print("\n=== INSTRUCTIONS ===")
    print("1. Run this test script to verify phone cleaning works correctly")
    print("2. If the cleaning looks good, run your main sync script")
    print("3. Check the enhanced error logs for any remaining data issues")
    print("4. The Mobile field errors should be significantly reduced")
