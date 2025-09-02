from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import json

@transform(
    # Replace these with your actual RIDs
    geojson_input=Input("ri.foundry.main.dataset.your-geojson-rid-here"),
    table_input=Input("ri.foundry.main.dataset.your-table-rid-here"),
    output_geojson=Output("ri.foundry.main.dataset.your-output-rid-here")
)
def merge_geojson_with_table(geojson_input, table_input, output_geojson):
    """
    Merges a GeoJSON dataset with a regular Foundry dataset based on county names.
    
    This version handles GeoJSON stored as an unstructured dataset with proper error handling.
    """
    
    try:
        # Step 1: Read the GeoJSON dataset and get Spark session
        geojson_df = geojson_input.dataframe()
        
        # Get Spark session from the dataframe
        spark = geojson_df.sparkSession
        
        # Check if the dataframe has content
        if geojson_df.count() == 0:
            raise ValueError("GeoJSON dataset is empty")
        
        # Debug: Print schema and first row to understand structure
        print("GeoJSON DataFrame Schema:")
        geojson_df.printSchema()
        print(f"Number of rows in GeoJSON dataset: {geojson_df.count()}")
        print("Column names:", geojson_df.columns)
        
        # Handle different possible schema formats for GeoJSON storage
        geojson_data = None
        
        if 'value' in geojson_df.columns:
            # Most common: Unstructured format - content is in 'value' column
            print("Found 'value' column - processing as unstructured dataset")
            geojson_content = geojson_df.select("value").collect()
            
            # Parse the GeoJSON content
            if len(geojson_content) == 1:
                # Single file case
                geojson_data = json.loads(geojson_content[0][0])
            else:
                # Multiple files - concatenate them
                merged_features = []
                for row in geojson_content:
                    data = json.loads(row[0])
                    if 'features' in data:
                        merged_features.extend(data['features'])
                    elif 'type' in data and data['type'] == 'Feature':
                        # Single feature
                        merged_features.append(data)
                
                # Create a combined GeoJSON structure
                geojson_data = {
                    "type": "FeatureCollection",
                    "features": merged_features
                }
                
        elif 'content' in geojson_df.columns:
            # Alternative unstructured format
            print("Found 'content' column - processing as alternative unstructured format")
            geojson_content = geojson_df.select("content").first()[0]
            geojson_data = json.loads(geojson_content)
            
        elif 'text' in geojson_df.columns:
            # Text column format
            print("Found 'text' column - processing as text format")
            geojson_content = geojson_df.select("text").first()[0]
            geojson_data = json.loads(geojson_content)
            
        else:
            # Try to use the first column regardless of name
            print(f"No standard column found. Attempting to use first column: {geojson_df.columns[0]}")
            first_col = geojson_df.columns[0]
            geojson_content = geojson_df.select(first_col).collect()
            
            if len(geojson_content) == 1:
                # Try to parse as JSON
                try:
                    geojson_data = json.loads(geojson_content[0][0])
                except:
                    # Maybe it's binary data, try to decode
                    content = geojson_content[0][0]
                    if isinstance(content, bytes):
                        content = content.decode('utf-8')
                    geojson_data = json.loads(content)
            else:
                raise ValueError(f"Unable to parse GeoJSON from column {first_col}")
        
        if not geojson_data:
            raise ValueError("Failed to parse GeoJSON data from dataset")
        
        # Validate GeoJSON structure
        if 'features' not in geojson_data:
            if 'type' in geojson_data and geojson_data['type'] == 'Feature':
                # Single feature - wrap in FeatureCollection
                geojson_data = {
                    "type": "FeatureCollection",
                    "features": [geojson_data]
                }
            else:
                raise ValueError("Invalid GeoJSON structure - no features found")
        
        # Step 2: Extract features and create a DataFrame from GeoJSON properties
        features = geojson_data.get('features', [])
        
        if not features:
            raise ValueError("No features found in GeoJSON file")
        
        print(f"Found {len(features)} features in GeoJSON")
        
        # Extract properties from each feature
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i  # Add index to maintain feature order
            properties_list.append(properties)
        
        # Create DataFrame from properties
        geojson_props_df = spark.createDataFrame(properties_list)
        
        print("GeoJSON Properties DataFrame Schema:")
        geojson_props_df.printSchema()
        print("Sample properties:", geojson_props_df.first())
        
        # Check for county name column (case-insensitive)
        county_col_geojson = None
        for col in geojson_props_df.columns:
            if col.upper() in ['COUNTY_NAME', 'COUNTYNAME', 'COUNTY', 'NAME']:
                county_col_geojson = col
                print(f"Found county column in GeoJSON: {col}")
                break
        
        if not county_col_geojson:
            raise ValueError(f"No county name column found in GeoJSON properties. Available columns: {geojson_props_df.columns}")
        
        # Standardize the county name for matching
        geojson_props_df = geojson_props_df.withColumn(
            'COUNTY_NAME_CLEAN', 
            F.upper(F.trim(F.col(county_col_geojson)))
        )
        
        # Show sample of cleaned county names
        print("Sample county names from GeoJSON:")
        geojson_props_df.select(county_col_geojson, 'COUNTY_NAME_CLEAN').show(5, truncate=False)
        
    except Exception as e:
        print(f"Error details: {str(e)}")
        raise ValueError(f"Error processing GeoJSON input: {str(e)}")
    
    try:
        # Step 3: Prepare the table dataset for joining
        table_df = table_input.dataframe()
        
        print("\nTable DataFrame Schema:")
        table_df.printSchema()
        print(f"Number of rows in table dataset: {table_df.count()}")
        
        # Check if table has content
        if table_df.count() == 0:
            raise ValueError("Table dataset is empty")
        
        # Find county column in table (case-insensitive)
        county_col_table = None
        for col in table_df.columns:
            if col.lower() in ['county', 'county_name', 'countyname', 'name']:
                county_col_table = col
                print(f"Found county column in table: {col}")
                break
        
        if not county_col_table:
            raise ValueError(f"No county column found in table dataset. Available columns: {table_df.columns}")
        
        # Standardize the county column for matching
        table_df = table_df.withColumn(
            'county_clean', 
            F.upper(F.trim(F.col(county_col_table)))
        )
        
        # Show sample of cleaned county names from table
        print("Sample county names from table:")
        table_df.select(county_col_table, 'county_clean').show(5, truncate=False)
        
        # Get all columns from table dataset except the join columns
        table_columns = [col for col in table_df.columns 
                        if col not in [county_col_table, 'county_clean']]
        
        print(f"Columns to be added from table: {table_columns}")
        
    except Exception as e:
        print(f"Error details: {str(e)}")
        raise ValueError(f"Error processing table input: {str(e)}")
    
    try:
        # Step 4: Perform the merge (left join to preserve all GeoJSON features)
        print("\nPerforming join operation...")
        merged_df = geojson_props_df.join(
            table_df,
            geojson_props_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Log join statistics
        total_features = geojson_props_df.count()
        matched_features = merged_df.filter(F.col('county_clean').isNotNull()).count()
        unmatched_features = total_features - matched_features
        
        print(f"\nJoin statistics:")
        print(f"  Total features: {total_features}")
        print(f"  Matched features: {matched_features}")
        print(f"  Unmatched features: {unmatched_features}")
        
        if unmatched_features > 0:
            print("\nUnmatched counties from GeoJSON:")
            unmatched = merged_df.filter(F.col('county_clean').isNull()).select(county_col_geojson).distinct()
            unmatched.show(20, truncate=False)
        
        # Select columns for final output
        final_columns = ['_feature_index'] + \
                       [col for col in geojson_props_df.columns 
                        if col not in ['COUNTY_NAME_CLEAN', '_feature_index']] + \
                       table_columns
        
        merged_df = merged_df.select(*final_columns)
        
        # Step 5: Convert back to GeoJSON format
        print("\nConverting back to GeoJSON format...")
        
        # Collect the merged data
        merged_properties = merged_df.collect()
        
        # Create a mapping of feature index to merged properties
        properties_map = {}
        for row in merged_properties:
            row_dict = row.asDict()
            feature_idx = row_dict.pop('_feature_index')
            # Remove None values to keep GeoJSON clean
            cleaned_dict = {k: v for k, v in row_dict.items() if v is not None}
            properties_map[feature_idx] = cleaned_dict
        
        # Update the original GeoJSON features with merged properties
        output_features = []
        for i, feature in enumerate(features):
            new_feature = {
                "type": feature.get("type", "Feature"),
                "geometry": feature.get("geometry", {})
            }
            
            # Update properties with merged data
            if i in properties_map:
                new_feature["properties"] = properties_map[i]
            else:
                # Keep original properties if no merge occurred
                new_feature["properties"] = feature.get("properties", {})
            
            # Preserve any other feature attributes (like 'id' if present)
            for key, value in feature.items():
                if key not in ["type", "geometry", "properties"]:
                    new_feature[key] = value
            
            output_features.append(new_feature)
        
        # Create the output GeoJSON structure
        output_geojson_data = {
            "type": geojson_data.get("type", "FeatureCollection"),
            "features": output_features
        }
        
        # Add any other top-level properties from original GeoJSON (like 'crs' if present)
        for key, value in geojson_data.items():
            if key not in ["type", "features"]:
                output_geojson_data[key] = value
        
        # Step 6: Convert to JSON string and save as new dataset
        output_json_string = json.dumps(output_geojson_data, indent=2)
        
        # Create a DataFrame with the JSON string
        output_df = spark.createDataFrame(
            [(output_json_string,)], 
            schema=StructType([StructField("value", StringType(), True)])
        )
        
        # Write the output
        output_geojson.write_dataframe(output_df)
        
        print(f"\n✅ Successfully merged GeoJSON with table data.")
        print(f"   Output contains {len(output_features)} features.")
        print(f"   {matched_features} features were enriched with table data.")
        
    except Exception as e:
        print(f"Error details: {str(e)}")
        raise ValueError(f"Error during merge operation: {str(e)}")