#!/usr/bin/env python3
"""
Local test runner for GeoJSON merger functions.
This script allows testing the functions without Foundry by using mock implementations.
"""

import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the mock transforms and the main functions
from mock_transforms import Input, Output, transform
from geojson_merger import merge_geojson_with_table, merge_geojson_with_table_distributed


def run_local_test():
    """Run a local test of the GeoJSON merger functions."""
    print("🚀 Starting local test of GeoJSON merger functions...")
    
    # Create mock inputs and outputs
    geojson_input = Input("/mock/geojson/dataset")
    table_input = Input("/mock/table/dataset")
    output_geojson = Output("/mock/output/geojson/dataset")
    
    print("\n📊 Input Data Summary:")
    print(f"GeoJSON input: {geojson_input.dataframe().count()} rows")
    print(f"Table input: {table_input.dataframe().count()} rows")
    
    print("\n🔧 Testing merge_geojson_with_table function...")
    try:
        # Test the main function
        merge_geojson_with_table(geojson_input, table_input, output_geojson)
        print("✅ merge_geojson_with_table completed successfully!")
        
        # Check the output
        output_df = output_geojson.get_written_dataframe()
        if output_df:
            print(f"📝 Output written: {output_df.count()} rows")
            
            # Parse the output JSON to verify structure
            output_content = output_df.select("value").first()[0]
            output_geojson_data = json.loads(output_content)
            
            print(f"🗺️  Output GeoJSON type: {output_geojson_data.get('type')}")
            print(f"📍 Number of features: {len(output_geojson_data.get('features', []))}")
            
            # Show sample properties from first feature
            if output_geojson_data.get('features'):
                first_feature = output_geojson_data['features'][0]
                properties = first_feature.get('properties', {})
                print(f"🏛️  First feature county: {properties.get('COUNTY_NAME')}")
                print(f"📈 Additional data columns: {[k for k in properties.keys() if k not in ['COUNTY_NAME', 'POPULATION', 'AREA']]}")
        
    except Exception as e:
        print(f"❌ Error in merge_geojson_with_table: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n🔧 Testing merge_geojson_with_table_distributed function...")
    try:
        # Test the distributed function
        merge_geojson_with_table_distributed(geojson_input, table_input, output_geojson)
        print("✅ merge_geojson_with_table_distributed completed successfully!")
        
        # Check the output
        output_df = output_geojson.get_written_dataframe()
        if output_df:
            print(f"📝 Output written: {output_df.count()} rows")
            
            # Parse the output JSON to verify structure
            output_content = output_df.select("value").first()[0]
            output_geojson_data = json.loads(output_content)
            
            print(f"🗺️  Output GeoJSON type: {output_geojson_data.get('type')}")
            print(f"📍 Number of features: {len(output_geojson_data.get('features', []))}")
            
            # Verify geometry preservation
            if output_geojson_data.get('features'):
                first_feature = output_geojson_data['features'][0]
                geometry = first_feature.get('geometry', {})
                print(f"📐 Geometry type: {geometry.get('type')}")
                print(f"📍 Coordinates count: {len(geometry.get('coordinates', []))}")
        
    except Exception as e:
        print(f"❌ Error in merge_geojson_with_table_distributed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n🎉 Local testing completed!")


def create_sample_data():
    """Create sample data files for testing."""
    print("\n📁 Creating sample data files...")
    
    # Sample GeoJSON file
    sample_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
                },
                "properties": {
                    "COUNTY_NAME": "Los Angeles",
                    "POPULATION": 1000000,
                    "AREA": 500.5
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]]
                },
                "properties": {
                    "COUNTY_NAME": "Orange County",
                    "POPULATION": 500000,
                    "AREA": 300.2
                }
            }
        ]
    }
    
    with open("sample_geojson.json", "w") as f:
        json.dump(sample_geojson, f, indent=2)
    
    # Sample table data
    sample_table_data = [
        {"county": "Los Angeles", "crime_rate": 0.05, "unemployment": 0.08},
        {"county": "Orange County", "crime_rate": 0.03, "unemployment": 0.06},
        {"county": "San Diego", "crime_rate": 0.04, "unemployment": 0.07}
    ]
    
    with open("sample_table.json", "w") as f:
        json.dump(sample_table_data, f, indent=2)
    
    print("✅ Sample data files created:")
    print("   - sample_geojson.json")
    print("   - sample_table.json")


if __name__ == "__main__":
    print("=" * 60)
    print("🌍 GeoJSON Merger - Local Test Runner")
    print("=" * 60)
    
    # Create sample data
    create_sample_data()
    
    # Run local tests
    run_local_test()
    
    print("\n" + "=" * 60)
    print("📚 Next steps:")
    print("1. Review the sample data files created")
    print("2. Run unit tests: python -m pytest test_geojson_merger.py -v")
    print("3. Run with coverage: python -m pytest --cov=geojson_merger")
    print("4. Update paths in geojson_merger.py for your Foundry environment")
    print("=" * 60)
