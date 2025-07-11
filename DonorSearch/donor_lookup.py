import json
import requests
import urllib.parse
import time
import csv
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re

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

def parse_donation_table(html_content: str) -> List[Dict[str, Any]]:
    """
    Parse the donation table from OpenSecrets HTML content.
    
    Args:
        html_content (str): HTML content from OpenSecrets page
        
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
                donation = {
                    'category': cells[0].get_text(strip=True),
                    'contributor': cells[1].get_text(strip=True),
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
                contributor_text = donation['contributor']
                state_match = re.search(r'([A-Z]{2})\s+\d{5}', contributor_text)
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
        donations = parse_donation_table(response.text)
        
        return {
            'name': name,
            'url': url,
            'status_code': response.status_code,
            'donations': donations,
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
            'donations': [],
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

def export_donations_to_csv(results: List[Dict[str, Any]], csv_file: str) -> None:
    """
    Export all donation records to a CSV file for easier analysis.
    
    Args:
        results (List[Dict[str, Any]]): Results from donor lookup queries
        csv_file (str): Path to save the CSV file
    """
    all_donations = []
    
    # Collect all donations from all results
    for result in results:
        if result['success'] and result['donations']:
            for donation in result['donations']:
                # Add the searched name to each donation record
                donation_with_name = donation.copy()
                donation_with_name['searched_name'] = result['name']
                all_donations.append(donation_with_name)
    
    if not all_donations:
        print("No donations found to export to CSV.")
        return
    
    # Define CSV headers
    headers = [
        'searched_name', 'category', 'contributor', 'employer', 'occupation',
        'date', 'amount', 'amount_numeric', 'recipient', 'jurisdiction'
    ]
    
    # Write to CSV
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        
        for donation in all_donations:
            # Ensure all required fields exist
            row = {header: donation.get(header, '') for header in headers}
            writer.writerow(row)
    
    print(f"Exported {len(all_donations)} donation records to {csv_file}")

def extract_donor_key(donation: Dict[str, Any]) -> str:
    """
    Create a unique key for grouping donors based on name, state, and employer/occupation.
    
    Args:
        donation (Dict[str, Any]): Donation record
        
    Returns:
        str: Unique donor key
    """
    contributor = donation.get('contributor', '').strip()
    state = donation.get('contributor_state', '').strip()
    employer = donation.get('employer', '').strip()
    occupation = donation.get('occupation', '').strip()
    
    # Extract just the name part (before address)
    name_parts = contributor.split('\n')
    name = name_parts[0].strip() if name_parts else contributor
    
    # Create a normalized key
    key = f"{name}|{state}|{employer}|{occupation}".upper()
    return key

def group_donors_and_analyze_party(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Group donors by unique identifiers and analyze their party preferences.
    
    Args:
        results (List[Dict[str, Any]]): Results from donor lookup queries
        
    Returns:
        Dict[str, Dict[str, Any]]: Grouped donor analysis
    """
    donor_groups = {}
    
    # Collect all donations from all results
    for result in results:
        if result['success'] and result['donations']:
            for donation in result['donations']:
                donor_key = extract_donor_key(donation)
                
                if donor_key not in donor_groups:
                    donor_groups[donor_key] = {
                        'donor_info': {
                            'name': donation.get('contributor', '').split('\n')[0].strip(),
                            'state': donation.get('contributor_state', ''),
                            'employer': donation.get('employer', ''),
                            'occupation': donation.get('occupation', ''),
                            'searched_name': result['name']
                        },
                        'donations': [],
                        'party_analysis': {
                            'democratic_count': 0,
                            'republican_count': 0,
                            'other_count': 0,
                            'democratic_amount': 0,
                            'republican_amount': 0,
                            'other_amount': 0,
                            'total_donations': 0,
                            'total_amount': 0
                        }
                    }
                
                # Add donation to group
                donor_groups[donor_key]['donations'].append(donation)
                
                # Update party analysis
                party = donation.get('recipient_party', '')
                amount = donation.get('amount_numeric', 0)
                
                analysis = donor_groups[donor_key]['party_analysis']
                analysis['total_donations'] += 1
                analysis['total_amount'] += amount
                
                if party == 'D':
                    analysis['democratic_count'] += 1
                    analysis['democratic_amount'] += amount
                elif party == 'R':
                    analysis['republican_count'] += 1
                    analysis['republican_amount'] += amount
                else:
                    analysis['other_count'] += 1
                    analysis['other_amount'] += amount
    
    # Calculate percentages and party preference
    for donor_key, group in donor_groups.items():
        analysis = group['party_analysis']
        total = analysis['total_donations']
        
        if total > 0:
            analysis['democratic_percentage'] = (analysis['democratic_count'] / total) * 100
            analysis['republican_percentage'] = (analysis['republican_count'] / total) * 100
            analysis['other_percentage'] = (analysis['other_count'] / total) * 100
            
            # Determine party preference
            if analysis['democratic_percentage'] >= 80:
                analysis['party_preference'] = 'Strong Democratic'
            elif analysis['republican_percentage'] >= 80:
                analysis['party_preference'] = 'Strong Republican'
            elif analysis['democratic_percentage'] > analysis['republican_percentage']:
                analysis['party_preference'] = 'Lean Democratic'
            elif analysis['republican_percentage'] > analysis['democratic_percentage']:
                analysis['party_preference'] = 'Lean Republican'
            else:
                analysis['party_preference'] = 'Mixed/Independent'
        else:
            analysis['democratic_percentage'] = 0
            analysis['republican_percentage'] = 0
            analysis['other_percentage'] = 0
            analysis['party_preference'] = 'Unknown'
    
    return donor_groups

def export_grouped_donors_to_csv(donor_groups: Dict[str, Dict[str, Any]], csv_file: str) -> None:
    """
    Export grouped donor analysis to CSV.
    
    Args:
        donor_groups (Dict[str, Dict[str, Any]]): Grouped donor data
        csv_file (str): Path to save the CSV file
    """
    if not donor_groups:
        print("No donor groups found to export.")
        return
    
    headers = [
        'donor_name', 'state', 'employer', 'occupation', 'searched_name',
        'total_donations', 'total_amount', 
        'democratic_count', 'democratic_amount', 'democratic_percentage',
        'republican_count', 'republican_amount', 'republican_percentage',
        'other_count', 'other_amount', 'other_percentage',
        'party_preference'
    ]
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        
        for donor_key, group in donor_groups.items():
            info = group['donor_info']
            analysis = group['party_analysis']
            
            row = {
                'donor_name': info['name'],
                'state': info['state'],
                'employer': info['employer'],
                'occupation': info['occupation'],
                'searched_name': info['searched_name'],
                'total_donations': analysis['total_donations'],
                'total_amount': analysis['total_amount'],
                'democratic_count': analysis['democratic_count'],
                'democratic_amount': analysis['democratic_amount'],
                'democratic_percentage': round(analysis['democratic_percentage'], 1),
                'republican_count': analysis['republican_count'],
                'republican_amount': analysis['republican_amount'],
                'republican_percentage': round(analysis['republican_percentage'], 1),
                'other_count': analysis['other_count'],
                'other_amount': analysis['other_amount'],
                'other_percentage': round(analysis['other_percentage'], 1),
                'party_preference': analysis['party_preference']
            }
            writer.writerow(row)
    
    print(f"Exported {len(donor_groups)} grouped donors to {csv_file}")

def export_detailed_donations_with_grouping(results: List[Dict[str, Any]], csv_file: str) -> None:
    """
    Export detailed donation records with donor grouping information.
    
    Args:
        results (List[Dict[str, Any]]): Results from donor lookup queries
        csv_file (str): Path to save the CSV file
    """
    all_donations = []
    
    # Collect all donations with grouping keys
    for result in results:
        if result['success'] and result['donations']:
            for donation in result['donations']:
                donation_with_info = donation.copy()
                donation_with_info['searched_name'] = result['name']
                donation_with_info['donor_group_key'] = extract_donor_key(donation)
                all_donations.append(donation_with_info)
    
    if not all_donations:
        print("No donations found to export.")
        return
    
    headers = [
        'searched_name', 'donor_group_key', 'category', 'contributor', 'contributor_state',
        'employer', 'occupation', 'date', 'amount', 'amount_numeric', 
        'recipient', 'recipient_party', 'jurisdiction'
    ]
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        
        for donation in all_donations:
            row = {header: donation.get(header, '') for header in headers}
            writer.writerow(row)
    
    print(f"Exported {len(all_donations)} detailed donation records to {csv_file}")

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
    
    # Group donors and analyze party preferences
    print("\nGrouping donors and analyzing party preferences...")
    donor_groups = group_donors_and_analyze_party(results)
    
    # Export grouped donor analysis
    grouped_csv_file = "./donor_groups_analysis.csv"
    export_grouped_donors_to_csv(donor_groups, grouped_csv_file)
    
    # Export detailed donations with grouping information
    detailed_csv_file = "./detailed_donations_with_grouping.csv"
    export_detailed_donations_with_grouping(results, detailed_csv_file)
    
    # Export original format for backward compatibility
    original_csv_file = "./donor_lookup_results.csv"
    export_donations_to_csv(results, original_csv_file)
    
    # Print summary
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    total_donations = sum(r['total_donations'] for r in results if r['success'])
    total_amount = sum(r['total_amount'] for r in results if r['success'])
    
    # Analyze party preferences
    strong_dem = sum(1 for g in donor_groups.values() if g['party_analysis']['party_preference'] == 'Strong Democratic')
    strong_rep = sum(1 for g in donor_groups.values() if g['party_analysis']['party_preference'] == 'Strong Republican')
    lean_dem = sum(1 for g in donor_groups.values() if g['party_analysis']['party_preference'] == 'Lean Democratic')
    lean_rep = sum(1 for g in donor_groups.values() if g['party_analysis']['party_preference'] == 'Lean Republican')
    mixed = sum(1 for g in donor_groups.values() if g['party_analysis']['party_preference'] == 'Mixed/Independent')
    
    print(f"\nSummary:")
    print(f"Total names processed: {len(results)}")
    print(f"Successful queries: {successful}")
    print(f"Failed queries: {failed}")
    print(f"Total donations found: {total_donations}")
    print(f"Total donation amount: ${total_amount:,}")
    print(f"\nDonor Grouping:")
    print(f"Unique donors identified: {len(donor_groups)}")
    print(f"\nParty Preferences:")
    print(f"Strong Democratic: {strong_dem}")
    print(f"Lean Democratic: {lean_dem}")
    print(f"Strong Republican: {strong_rep}")
    print(f"Lean Republican: {lean_rep}")
    print(f"Mixed/Independent: {mixed}")
    print(f"\nFiles created:")
    print(f"- JSON results: {output_file}")
    print(f"- Grouped donor analysis: {grouped_csv_file}")
    print(f"- Detailed donations with grouping: {detailed_csv_file}")
    print(f"- Original format CSV: {original_csv_file}")