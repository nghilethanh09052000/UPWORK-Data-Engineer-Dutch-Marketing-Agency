#!/usr/bin/env python3
"""
Script to list all regions_served arrays from output JSON files.

This script reads all JSON files in the output/ directory and extracts
the regions_served field from each agency's data.
"""

import json
import os
from pathlib import Path
from typing import Dict, List


def load_regions_served(output_dir: str = "output") -> Dict[str, Dict]:
    """
    Load regions_served data from all JSON files in the output directory.
    
    Parameters
    ----------
    output_dir : str
        Directory containing the JSON output files
        
    Returns
    -------
    Dict[str, Dict]
        Dictionary mapping agency names to their regions_served data
    """
    regions_data = {}
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Error: Directory '{output_dir}' does not exist!")
        return regions_data
    
    # Get all JSON files (excluding _sample.json)
    json_files = sorted([f for f in output_path.glob("*.json") if f.name != "_sample.json"])
    
    if not json_files:
        print(f"⚠️  No JSON files found in '{output_dir}' directory!")
        return regions_data
    
    print(f"📂 Reading {len(json_files)} JSON files from '{output_dir}'...\n")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            agency_name = data.get('agency_name', json_file.stem)
            regions_served = data.get('regions_served', [])
            
            regions_data[agency_name] = {
                "filename": json_file.name,
                "regions_served": regions_served,
                "count": len(regions_served),
                "website_url": data.get('website_url', 'N/A')
            }
            
        except json.JSONDecodeError as e:
            print(f"⚠️  Error parsing {json_file.name}: {e}")
        except Exception as e:
            print(f"⚠️  Error reading {json_file.name}: {e}")
    
    return regions_data


def print_summary_table(regions_data: Dict[str, Dict]) -> None:
    """Print a formatted summary table of all regions_served."""
    print("=" * 100)
    print("REGIONS_SERVED - SUMMARY TABLE")
    print("=" * 100)
    print(f"{'Agency Name':<25} | {'Count':<6} | {'Regions Served'}")
    print("-" * 100)
    
    for agency_name in sorted(regions_data.keys()):
        info = regions_data[agency_name]
        regions_str = ", ".join(info['regions_served']) if info['regions_served'] else "(empty)"
        print(f"{agency_name:<25} | {info['count']:<6} | {regions_str}")
    
    print("=" * 100)
    print(f"\nTotal agencies: {len(regions_data)}")
    print()


def print_detailed_list(regions_data: Dict[str, Dict]) -> None:
    """Print detailed list with JSON format for each agency."""
    print("\n" + "=" * 100)
    print("REGIONS_SERVED - DETAILED LIST (JSON Format)")
    print("=" * 100)
    
    for agency_name in sorted(regions_data.keys()):
        info = regions_data[agency_name]
        print(f"\n{agency_name} ({info['filename']}):")
        print(f"  Website: {info['website_url']}")
        print(f"  Count: {info['count']}")
        print(f"  regions_served: {json.dumps(info['regions_served'], indent=4, ensure_ascii=False)}")
    
    print("\n" + "=" * 100)


def print_analysis(regions_data: Dict[str, Dict]) -> None:
    """Print analysis of the regions_served data."""
    print("\n" + "=" * 100)
    print("ANALYSIS")
    print("=" * 100)
    
    # Count agencies with landelijk
    has_landelijk = sum(1 for info in regions_data.values() if "landelijk" in info['regions_served'])
    
    # Count agencies with Randstad
    has_randstad = sum(1 for info in regions_data.values() if "Randstad" in info['regions_served'])
    
    # Count agencies with only provinces (no landelijk or Randstad)
    only_provinces = sum(1 for info in regions_data.values() 
                        if info['regions_served'] 
                        and "landelijk" not in info['regions_served'] 
                        and "Randstad" not in info['regions_served'])
    
    # Find agencies with most regions
    max_regions = max((info['count'] for info in regions_data.values()), default=0)
    agencies_with_max = [name for name, info in regions_data.items() if info['count'] == max_regions]
    
    # Check for invalid values
    valid_labels = {
        "landelijk", "Randstad",
        "Noord-Holland", "Zuid-Holland", "Noord-Brabant", "Gelderland",
        "Overijssel", "Limburg", "Flevoland", "Utrecht", "Groningen",
        "Friesland", "Drenthe", "Zeeland"
    }
    
    invalid_values = []
    for agency_name, info in regions_data.items():
        for region in info['regions_served']:
            if region not in valid_labels:
                invalid_values.append((agency_name, region))
    
    print(f"\n📊 Statistics:")
    print(f"  • Total agencies: {len(regions_data)}")
    print(f"  • Agencies with 'landelijk': {has_landelijk}")
    print(f"  • Agencies with 'Randstad': {has_randstad}")
    print(f"  • Agencies with only provinces: {only_provinces}")
    print(f"  • Maximum regions per agency: {max_regions}")
    print(f"  • Agencies with {max_regions} regions: {', '.join(agencies_with_max)}")
    
    if invalid_values:
        print(f"\n⚠️  Invalid values found:")
        for agency, value in invalid_values:
            print(f"    • {agency}: '{value}'")
    else:
        print(f"\n✅ All regions_served use valid, normalized labels!")
    
    print()


def export_to_json(regions_data: Dict[str, Dict], output_file: str = "regions_served_data.json") -> None:
    """Export regions_served data to a JSON file."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(regions_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Exported data to '{output_file}'")
    except Exception as e:
        print(f"❌ Error exporting to JSON: {e}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="List all regions_served arrays from output JSON files"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory containing JSON output files (default: output)"
    )
    parser.add_argument(
        "--format",
        choices=["table", "detailed", "both", "json"],
        default="both",
        help="Output format: table, detailed, both, or json (default: both)"
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export data to regions_served_data.json"
    )
    parser.add_argument(
        "--no-analysis",
        action="store_true",
        help="Skip analysis section"
    )
    
    args = parser.parse_args()
    
    # Load data
    regions_data = load_regions_served(args.output_dir)
    
    if not regions_data:
        print("❌ No data found!")
        return
    
    # Print output based on format
    if args.format in ["table", "both"]:
        print_summary_table(regions_data)
    
    if args.format in ["detailed", "both"]:
        print_detailed_list(regions_data)
    
    if args.format == "json":
        print(json.dumps(regions_data, indent=2, ensure_ascii=False))
    
    # Print analysis
    if not args.no_analysis:
        print_analysis(regions_data)
    
    # Export if requested
    if args.export:
        export_to_json(regions_data)


if __name__ == "__main__":
    main()

