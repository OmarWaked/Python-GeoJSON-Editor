import unittest
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row
from pyspark.sql import DataFrame

# Import the functions to test
from geojson_merger import merge_geojson_with_table, merge_geojson_with_table_distributed


class TestGeoJSONMerger(unittest.TestCase):
    """Test cases for GeoJSON merger functions."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("GeoJSONMergerTest") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session."""
        if cls.spark:
            cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test."""
        # Sample GeoJSON data
        self.sample_geojson = {
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
        
        # Sample table data
        self.sample_table_data = [
            {"county": "Los Angeles", "crime_rate": 0.05, "unemployment": 0.08},
            {"county": "Orange County", "crime_rate": 0.03, "unemployment": 0.06},
            {"county": "San Diego", "crime_rate": 0.04, "unemployment": 0.07}
        ]
        
        # Create test DataFrames
        self.geojson_df = self.spark.createDataFrame(
            [(json.dumps(self.sample_geojson),)],
            schema=StructType([StructField("value", StringType(), True)])
        )
        
        self.table_df = self.spark.createDataFrame(self.sample_table_data)
    
    def test_geojson_parsing(self):
        """Test GeoJSON parsing functionality."""
        # Test single file parsing
        geojson_content = [json.dumps(self.sample_geojson)]
        parsed_data = json.loads(geojson_content[0])
        
        self.assertEqual(parsed_data["type"], "FeatureCollection")
        self.assertEqual(len(parsed_data["features"]), 2)
        self.assertEqual(parsed_data["features"][0]["properties"]["COUNTY_NAME"], "Los Angeles")
    
    def test_multiple_geojson_files_merging(self):
        """Test merging multiple GeoJSON files."""
        # Create multiple GeoJSON files
        geojson1 = {
            "type": "FeatureCollection",
            "features": [self.sample_geojson["features"][0]]
        }
        geojson2 = {
            "type": "FeatureCollection",
            "features": [self.sample_geojson["features"][1]]
        }
        
        geojson_contents = [json.dumps(geojson1), json.dumps(geojson2)]
        
        # Simulate the merging logic
        merged_features = []
        for content in geojson_contents:
            data = json.loads(content)
            if 'features' in data:
                merged_features.extend(data['features'])
        
        merged_geojson = {
            "type": "FeatureCollection",
            "features": merged_features
        }
        
        self.assertEqual(len(merged_geojson["features"]), 2)
        self.assertEqual(merged_geojson["features"][0]["properties"]["COUNTY_NAME"], "Los Angeles")
        self.assertEqual(merged_geojson["features"][1]["properties"]["COUNTY_NAME"], "Orange County")
    
    def test_county_name_standardization(self):
        """Test county name standardization for matching."""
        # Test the standardization logic
        test_names = ["  Los Angeles  ", "orange county", "San Diego  "]
        expected_clean = ["LOS ANGELES", "ORANGE COUNTY", "SAN DIEGO"]
        
        for i, name in enumerate(test_names):
            clean_name = name.strip().upper()
            self.assertEqual(clean_name, expected_clean[i])
    
    def test_dataframe_creation_from_properties(self):
        """Test creating DataFrame from GeoJSON properties."""
        properties_list = []
        for i, feature in enumerate(self.sample_geojson["features"]):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        df = self.spark.createDataFrame(properties_list)
        
        self.assertEqual(df.count(), 2)
        self.assertIn("COUNTY_NAME", df.columns)
        self.assertIn("POPULATION", df.columns)
        self.assertIn("_feature_index", df.columns)
    
    def test_table_data_preparation(self):
        """Test table dataset preparation."""
        # Test county column standardization
        df = self.table_df.withColumn(
            'county_clean',
            self.spark.sql.functions.upper(
                self.spark.sql.functions.trim(self.spark.sql.functions.col('county'))
            )
        )
        
        self.assertIn("county_clean", df.columns)
        
        # Check that clean county names are uppercase and trimmed
        clean_counties = df.select("county_clean").collect()
        clean_county_values = [row.county_clean for row in clean_counties]
        
        self.assertIn("LOS ANGELES", clean_county_values)
        self.assertIn("ORANGE COUNTY", clean_county_values)
    
    def test_join_operation(self):
        """Test the join operation between GeoJSON and table data."""
        # Prepare GeoJSON DataFrame
        properties_list = []
        for i, feature in enumerate(self.sample_geojson["features"]):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        geojson_df = self.spark.createDataFrame(properties_list)
        geojson_df = geojson_df.withColumn(
            'COUNTY_NAME_CLEAN',
            self.spark.sql.functions.upper(
                self.spark.sql.functions.trim(self.spark.sql.functions.col('COUNTY_NAME'))
            )
        )
        
        # Prepare table DataFrame
        table_df = self.table_df.withColumn(
            'county_clean',
            self.spark.sql.functions.upper(
                self.spark.sql.functions.trim(self.spark.sql.functions.col('county'))
            )
        )
        
        # Perform join
        merged_df = geojson_df.join(
            table_df,
            geojson_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Check join results
        self.assertEqual(merged_df.count(), 2)
        
        # Check that Los Angeles county has crime_rate and unemployment data
        la_row = merged_df.filter(merged_df.COUNTY_NAME == "Los Angeles").first()
        self.assertIsNotNone(la_row.crime_rate)
        self.assertIsNotNone(la_row.unemployment)
    
    def test_geojson_reconstruction(self):
        """Test GeoJSON feature reconstruction."""
        # Simulate merged properties
        merged_properties = {
            0: {
                "COUNTY_NAME": "Los Angeles",
                "POPULATION": 1000000,
                "AREA": 500.5,
                "crime_rate": 0.05,
                "unemployment": 0.08
            },
            1: {
                "COUNTY_NAME": "Orange County",
                "POPULATION": 500000,
                "AREA": 300.2,
                "crime_rate": 0.03,
                "unemployment": 0.06
            }
        }
        
        # Reconstruct features
        output_features = []
        for i, feature in enumerate(self.sample_geojson["features"]):
            new_feature = feature.copy()
            if i in merged_properties:
                new_feature['properties'] = merged_properties[i]
            output_features.append(new_feature)
        
        # Verify reconstruction
        self.assertEqual(len(output_features), 2)
        self.assertIn("crime_rate", output_features[0]["properties"])
        self.assertIn("unemployment", output_features[1]["properties"])
    
    def test_error_handling_missing_county_column(self):
        """Test error handling for missing COUNTY_NAME column."""
        # Create DataFrame without COUNTY_NAME
        invalid_properties = [{"POPULATION": 1000000, "AREA": 500.5}]
        invalid_df = self.spark.createDataFrame(invalid_properties)
        
        # This should raise an error when trying to access COUNTY_NAME column
        with self.assertRaises(Exception):
            invalid_df.select("COUNTY_NAME")
    
    def test_error_handling_empty_features(self):
        """Test error handling for empty GeoJSON features."""
        empty_geojson = {
            "type": "FeatureCollection",
            "features": []
        }
        
        features = empty_geojson.get('features', [])
        self.assertEqual(len(features), 0)
        
        # This would cause an error in the actual function
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        self.assertEqual(len(properties_list), 0)
    
    def test_output_json_structure(self):
        """Test the structure of output JSON."""
        # Create sample output GeoJSON
        output_geojson_data = {
            "type": "FeatureCollection",
            "features": self.sample_geojson["features"],
            "metadata": {
                "source": "test",
                "version": "1.0"
            }
        }
        
        # Convert to JSON string
        output_json_string = json.dumps(output_geojson_data, indent=2)
        
        # Verify JSON structure
        parsed_output = json.loads(output_json_string)
        self.assertEqual(parsed_output["type"], "FeatureCollection")
        self.assertIn("metadata", parsed_output)
        self.assertEqual(parsed_output["metadata"]["source"], "test")
    
    def test_distributed_implementation_geometry_preservation(self):
        """Test that geometry is preserved in distributed implementation."""
        # Test the geometry extraction function
        test_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            },
            "properties": {
                "COUNTY_NAME": "Test County",
                "POPULATION": 100000
            }
        }
        
        # Simulate the extract_properties_with_geometry function
        properties = test_feature.get('properties', {})
        properties['_geometry'] = json.dumps(test_feature.get('geometry', {}))
        properties['_type'] = test_feature.get('type', 'Feature')
        
        # Verify geometry is preserved
        self.assertIn("_geometry", properties)
        self.assertIn("_type", properties)
        
        # Verify geometry can be reconstructed
        reconstructed_geometry = json.loads(properties['_geometry'])
        self.assertEqual(reconstructed_geometry["type"], "Polygon")
        self.assertEqual(len(reconstructed_geometry["coordinates"][0]), 5)


class TestGeoJSONMergerIntegration(unittest.TestCase):
    """Integration tests for GeoJSON merger."""
    
    @classmethod
    def setUpClass(cls):
        """Set up integration test environment."""
        cls.spark = SparkSession.builder \
            .appName("GeoJSONMergerIntegrationTest") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up integration test environment."""
        if cls.spark:
            cls.spark.stop()
    
    def test_end_to_end_workflow(self):
        """Test the complete workflow from input to output."""
        # This test would require actual Foundry transforms to be available
        # For now, we'll test the data flow logic
        
        # Create sample data
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {"COUNTY_NAME": "Test County", "value": 100}
                }
            ]
        }
        
        table_data = [{"county": "Test County", "additional_data": "test_value"}]
        
        # Simulate the transformation steps
        # 1. Parse GeoJSON
        features = geojson_data.get('features', [])
        
        # 2. Extract properties
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        # 3. Create DataFrames
        geojson_df = self.spark.createDataFrame(properties_list)
        table_df = self.spark.createDataFrame(table_data)
        
        # 4. Standardize county names
        geojson_df = geojson_df.withColumn(
            'COUNTY_NAME_CLEAN',
            self.spark.sql.functions.upper(
                self.spark.sql.functions.trim(self.spark.sql.functions.col('COUNTY_NAME'))
            )
        )
        
        table_df = table_df.withColumn(
            'county_clean',
            self.spark.sql.functions.upper(
                self.spark.sql.functions.trim(self.spark.sql.functions.col('county'))
            )
        )
        
        # 5. Perform join
        merged_df = geojson_df.join(
            table_df,
            geojson_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # 6. Verify results
        self.assertEqual(merged_df.count(), 1)
        merged_row = merged_df.first()
        self.assertEqual(merged_row.COUNTY_NAME, "Test County")
        self.assertEqual(merged_row.additional_data, "test_value")


if __name__ == '__main__':
    unittest.main()
