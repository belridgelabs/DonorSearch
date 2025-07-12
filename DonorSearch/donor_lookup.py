import json
import requests
import urllib.parse
import time
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
from difflib import SequenceMatcher

def extract_first_name_and_split(contributor_full: str, original_name: str) -> tuple[str, str]:
    """
    Enhanced function to split name from address using sophisticated logic.
    
    The full name is known in advance (first_name, last_name), and the input string 
    begins with the name directly followed by the address (with no delimiter).
    
    Logic:
    1. Extract everything before the first occurrence of the first name
    2. If this prefix (case-insensitive) starts with the last name followed by a comma, keep it as the parsed name
    3. If not, extract everything before the last occurrence of the last name instead, and use that as the parsed name
    
    Args:
        contributor_full (str): Full contributor string like "KAUR, AASEESDUNWOODY, GA 30360"
        original_name (str): Original name from the query
        
    Returns:
        tuple[str, str]: (name_part, address_part)
    """
    # Extract first and last names from original name
    first_name = ''
    last_name = ''
    
    if ',' in original_name:
        # Format: "LAST, FIRST"
        parts = original_name.split(',')
        if len(parts) >= 2:
            last_name = parts[0].strip()
            first_name = parts[1].strip().split()[0]  # Get first word after comma
        else:
            # Single name after comma or malformed
            first_name = parts[0].strip().split()[0]
            last_name = ''
    else:
        # Format: "FIRST LAST" or single name
        name_parts = original_name.strip().split()
        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = name_parts[-1]  # Take the last word as last name
        else:
            first_name = name_parts[0] if name_parts else ''
            last_name = ''
    
    # Capitalize names for case-insensitive matching
    first_name_upper = first_name.upper()
    last_name_upper = last_name.upper()
    contributor_upper = contributor_full.upper()
    
    # Step 1: Find the first occurrence of the first name
    first_name_index = contributor_upper.find(first_name_upper) if first_name_upper else -1
    
    if first_name_index != -1:
        # Extract everything before the first occurrence of the first name
        prefix_before_first_name = contributor_full[:first_name_index].strip()
        
        # Step 2: Check if this prefix starts with last name followed by comma (case-insensitive)
        if last_name_upper and prefix_before_first_name.upper().startswith(last_name_upper + ','):
            # Use the prefix + first name as the parsed name
            name_end_index = first_name_index + len(first_name_upper)
            name_part = contributor_full[:name_end_index].strip()
            address_part = contributor_full[name_end_index:].strip()
            return name_part, address_part
        
        # Step 3: If not, find the last occurrence of the last name
        if last_name_upper:
            last_name_index = contributor_upper.rfind(last_name_upper)
            if last_name_index != -1:
                # Use everything before the last occurrence of the last name
                name_part = contributor_full[:last_name_index + len(last_name_upper)].strip()
                address_part = contributor_full[last_name_index + len(last_name_upper):].strip()
                return name_part, address_part
        
        # Fallback: use the original logic (everything before first name + first name)
        name_end_index = first_name_index + len(first_name_upper)
        name_part = contributor_full[:name_end_index].strip()
        address_part = contributor_full[name_end_index:].strip()
        return name_part, address_part
    
    # If first name not found, try to find last name only
    if last_name_upper:
        last_name_index = contributor_upper.rfind(last_name_upper)
        if last_name_index != -1:
            name_part = contributor_full[:last_name_index + len(last_name_upper)].strip()
            address_part = contributor_full[last_name_index + len(last_name_upper):].strip()
            return name_part, address_part
    
    # Final fallback: return the whole string as name and empty address
    return contributor_full, ''

def extract_name_from_contributor(contributor_full: str, original_name: str = '') -> str:
    """
    Extract the name part from the full contributor string using first name splitting.
    
    Args:
        contributor_full (str): Full contributor string like "KAUR, AASEESDUNWOODY, GA 30360"
        original_name (str): Original name from the query
        
    Returns:
        str: Just the name part
    """
    if original_name:
        name_part, _ = extract_first_name_and_split(contributor_full, original_name)
        return name_part
    else:
        # Fallback to original logic if no original_name provided
        return contributor_full

def extract_address_from_contributor(contributor_full: str, original_name: str = '') -> str:
    """
    Extract the address part from the full contributor string using first name splitting.
    
    Args:
        contributor_full (str): Full contributor string like "KAUR, AASEESDUNWOODY, GA 30360"
        original_name (str): Original name from the query
        
    Returns:
        str: Just the address part
    """
    if original_name:
        _, address_part = extract_first_name_and_split(contributor_full, original_name)
        return address_part
    else:
        # Fallback to empty string if no original_name provided
        return ''

def calculate_address_similarity(addr1: str, addr2: str) -> float:
    """
    Calculate similarity between two addresses.
    
    Args:
        addr1 (str): First address
        addr2 (str): Second address
        
    Returns:
        float: Similarity ratio between 0 and 1
    """
    if not addr1 or not addr2:
        return 0.0
    
    # Normalize addresses for comparison
    addr1_norm = addr1.upper().strip()
    addr2_norm = addr2.upper().strip()
    
    return SequenceMatcher(None, addr1_norm, addr2_norm).ratio()

def group_donations_by_variants(donations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group donations by similar addresses into variants.
    
    Args:
        donations (List[Dict[str, Any]]): List of donation records
        
    Returns:
        List[Dict[str, Any]]: List of variant groups
    """
    if not donations:
        return []
    
    variants = []
    used_indices = set()
    
    for i, donation in enumerate(donations):
        if i in used_indices:
            continue
            
        # Start a new variant group with this donation
        variant = {
            'contributor_name': donation.get('contributor_name', ''),
            'contributor_address': donation.get('contributor_address', ''),
            'donations': [donation]
        }
        
        used_indices.add(i)
        
        # Find similar addresses (similarity >= 0.8 means 1-2 characters different)
        current_address = donation.get('contributor_address', '')
        
        for j, other_donation in enumerate(donations):
            if j in used_indices:
                continue
                
            other_address = other_donation.get('contributor_address', '')
            similarity = calculate_address_similarity(current_address, other_address)
            
            # Group if addresses are similar enough (allowing 1-2 character differences)
            if similarity >= 0.8:
                variant['donations'].append(other_donation)
                used_indices.add(j)
        
        variants.append(variant)
    
    return variants

def load_names_from_json(json_file_path: str) -> List[str]:
    """
    Load all names from the output.json file.
    
    Args:
        json_file_path (str): Path to the JSON file containing organization data
        
    Returns:
        List[str]: List of all unique names found in the JSON file
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    names = set()  # Use set to avoid duplicates
    
    # Extract names from all organizations
    for organization in data:
        if 'members' in organization:
            for member in organization['members']:
                if 'name' in member:
                    names.add(member['name'])
    
    return list(names)

def parse_donation_table(html_content: str, original_name: str = '') -> List[Dict[str, Any]]:
    """
    Parse the donation table from OpenSecrets HTML content.
    
    Args:
        html_content (str): HTML content from OpenSecrets page
        original_name (str): Original name used for the query
        
    Returns:
        List[Dict[str, Any]]: List of donation records
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    donations = []
    
    # Find the main data table
    table = soup.find('table')
    if not table:
        return donations
    
    # Find all rows in the table body
    rows = table.find_all('tr')
    
    # Skip header row and process data rows
    for row in rows[1:]:  # Skip the first row (header)
        cells = row.find_all('td')
        if len(cells) >= 8:  # Ensure we have enough columns
            try:
                contributor_full = cells[1].get_text(strip=True)
                
                # Extract name and address using original name for splitting
                contributor_name = extract_name_from_contributor(contributor_full, original_name)
                contributor_address = extract_address_from_contributor(contributor_full, original_name)
                
                donation = {
                    'category': cells[0].get_text(strip=True),
                    'contributor_name': contributor_name,
                    'contributor_address': contributor_address,
                    'employer': cells[2].get_text(strip=True),
                    'occupation': cells[3].get_text(strip=True),
                    'date': cells[4].get_text(strip=True),
                    'amount': cells[5].get_text(strip=True),
                    'recipient': cells[6].get_text(strip=True),
                    'jurisdiction': cells[7].get_text(strip=True)
                }
                
                # Clean up amount field - extract numeric value
                amount_text = donation['amount']
                amount_match = re.search(r'\$([\d,]+)', amount_text)
                if amount_match:
                    donation['amount_numeric'] = int(amount_match.group(1).replace(',', ''))
                else:
                    donation['amount_numeric'] = 0
                
                # Extract state from contributor address
                state_match = re.search(r'([A-Z]{2})\s+\d{5}', contributor_address)
                donation['contributor_state'] = state_match.group(1) if state_match else ''
                
                # Extract party affiliation from recipient
                recipient_text = donation['recipient']
                party_match = re.search(r'\(([DR])\)', recipient_text)
                donation['recipient_party'] = party_match.group(1) if party_match else ''
                
                donations.append(donation)
            except Exception as e:
                # Skip malformed rows
                continue
    
    return donations

def query_opensecrets_donor(name: str) -> Dict[str, Any]:
    """
    Query OpenSecrets donor lookup for a specific name and parse donation table.
    
    Args:
        name (str): The name to search for
        
    Returns:
        Dict[str, Any]: Dictionary containing the name, URL, and parsed donation data
    """
    # URL encode the name for the query parameter
    encoded_name = urllib.parse.quote_plus(name)
    url = f"https://www.opensecrets.org/donor-lookup/results?name={encoded_name}"
    
    try:
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the donation table from HTML
        donations = parse_donation_table(response.text, name)
        
        # Group donations by variants (similar addresses)
        variants = group_donations_by_variants(donations)
        
        return {
            'name': name,
            'url': url,
            'status_code': response.status_code,
            'variants': variants,
            'total_donations': len(donations),
            'total_amount': sum(d.get('amount_numeric', 0) for d in donations),
            'success': True,
            'error': None
        }
        
    except requests.exceptions.RequestException as e:
        return {
            'name': name,
            'url': url,
            'status_code': None,
            'variants': [],
            'total_donations': 0,
            'total_amount': 0,
            'success': False,
            'error': str(e)
        }

def process_all_donors(json_file_path: str, delay_seconds: float = 1.0) -> List[Dict[str, Any]]:
    """
    Process all names from the JSON file and query OpenSecrets for each.
    
    Args:
        json_file_path (str): Path to the JSON file containing organization data
        delay_seconds (float): Delay between requests to be respectful to the server
        
    Returns:
        List[Dict[str, Any]]: List of results for each name queried
    """
    names = load_names_from_json(json_file_path)
    results = []
    
    print(f"Found {len(names)} unique names to process...")
    
    for i, name in enumerate(names, 1):
        print(f"Processing {i}/{len(names)}: {name}")
        
        result = query_opensecrets_donor(name)
        results.append(result)
        
        if result['success']:
            print(f"  ✓ Found {result['total_donations']} donations, Total: ${result['total_amount']:,}")
        else:
            print(f"  ✗ Error: {result['error']}")
        
        # Add delay between requests to be respectful
        if i < len(names):  # Don't delay after the last request
            time.sleep(delay_seconds)
    
    return results

def save_results_to_file(results: List[Dict[str, Any]], output_file: str) -> None:
    """
    Save the results to a JSON file.
    
    Args:
        results (List[Dict[str, Any]]): Results from donor lookup queries
        output_file (str): Path to save the results
    """
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(results, file, indent=2, ensure_ascii=False)
    
    print(f"Results saved to {output_file}")

# Example usage
if __name__ == "__main__":
    # Path to the input JSON file
    input_file = "./output.json"
    
    # Path for the output file
    output_file = "./donor_lookup_results.json"
    
    # Process all donors
    results = process_all_donors(input_file, delay_seconds=1.0)
    
    # Save results
    save_results_to_file(results, output_file)
    
    # Print summary
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    total_donations = sum(r['total_donations'] for r in results if r['success'])
    total_amount = sum(r['total_amount'] for r in results if r['success'])
    
    # Count total variants
    total_variants = sum(len(r['variants']) for r in results if r['success'])
    
    print(f"\nSummary:")
    print(f"Total names processed: {len(results)}")
    print(f"Successful queries: {successful}")
    print(f"Failed queries: {failed}")
    print(f"Total donations found: {total_donations}")
    print(f"Total donation amount: ${total_amount:,}")
    print(f"Total variants (grouped by similar addresses): {total_variants}")
    print(f"\nFiles created:")
    print(f"- JSON results with variants: {output_file}")