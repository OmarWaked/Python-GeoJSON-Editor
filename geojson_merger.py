from transforms.api import transform, Input, Output, FileInput
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
    
    # Get Spark context
    spark = geojson_input.spark_session
    
    try:
        # Step 1: Read the GeoJSON dataset
        geojson_df = geojson_input.dataframe()
        
        # Check if the dataframe has content
        if geojson_df.count() == 0:
            raise ValueError("GeoJSON dataset is empty")
        
        # Handle different possible schema formats for GeoJSON storage
        if 'value' in geojson_df.columns:
            # Unstructured format - content is in 'value' column
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
            geojson_content = geojson_df.select("content").first()[0]
            geojson_data = json.loads(geojson_content)
        else:
            # Try to collect all content as string
            first_row = geojson_df.first()
            if first_row:
                # Try to parse the first column as JSON
                geojson_data = json.loads(str(first_row[0]))
            else:
                raise ValueError("Unable to parse GeoJSON content from dataset")
        
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
        
        # Extract properties from each feature
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i  # Add index to maintain feature order
            properties_list.append(properties)
        
        # Create DataFrame from properties
        geojson_props_df = spark.createDataFrame(properties_list)
        
        # Check for county name column (case-insensitive)
        county_col_geojson = None
        for col in geojson_props_df.columns:
            if col.upper() in ['COUNTY_NAME', 'COUNTYNAME', 'COUNTY', 'NAME']:
                county_col_geojson = col
                break
        
        if not county_col_geojson:
            raise ValueError(f"No county name column found in GeoJSON properties. Available columns: {geojson_props_df.columns}")
        
        # Standardize the county name for matching
        geojson_props_df = geojson_props_df.withColumn(
            'COUNTY_NAME_CLEAN', 
            F.upper(F.trim(F.col(county_col_geojson)))
        )
        
    except Exception as e:
        raise ValueError(f"Error processing GeoJSON input: {str(e)}")
    
    try:
        # Step 3: Prepare the table dataset for joining
        table_df = table_input.dataframe()
        
        # Check if table has content
        if table_df.count() == 0:
            raise ValueError("Table dataset is empty")
        
        # Find county column in table (case-insensitive)
        county_col_table = None
        for col in table_df.columns:
            if col.lower() in ['county', 'county_name', 'countyname', 'name']:
                county_col_table = col
                break
        
        if not county_col_table:
            raise ValueError(f"No county column found in table dataset. Available columns: {table_df.columns}")
        
        # Standardize the county column for matching
        table_df = table_df.withColumn(
            'county_clean', 
            F.upper(F.trim(F.col(county_col_table)))
        )
        
        # Get all columns from table dataset except the join columns
        table_columns = [col for col in table_df.columns 
                        if col not in [county_col_table, 'county_clean']]
        
    except Exception as e:
        raise ValueError(f"Error processing table input: {str(e)}")
    
    try:
        # Step 4: Perform the merge (left join to preserve all GeoJSON features)
        merged_df = geojson_props_df.join(
            table_df,
            geojson_props_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Log join statistics
        total_features = geojson_props_df.count()
        matched_features = merged_df.filter(F.col('county_clean').isNotNull()).count()
        print(f"Join statistics: {matched_features}/{total_features} features matched")
        
        # Select columns for final output
        final_columns = ['_feature_index'] + \
                       [col for col in geojson_props_df.columns 
                        if col not in ['COUNTY_NAME_CLEAN', '_feature_index']] + \
                       table_columns
        
        merged_df = merged_df.select(*final_columns)
        
        # Step 5: Convert back to GeoJSON format
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
            
            # Preserve any other feature attributes
            for key, value in feature.items():
                if key not in ["type", "geometry", "properties"]:
                    new_feature[key] = value
            
            output_features.append(new_feature)
        
        # Create the output GeoJSON structure
        output_geojson_data = {
            "type": geojson_data.get("type", "FeatureCollection"),
            "features": output_features
        }
        
        # Add any other top-level properties from original GeoJSON
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
        
        print(f"Successfully merged GeoJSON with table data. Output contains {len(output_features)} features.")
        
    except Exception as e:
        raise ValueError(f"Error during merge operation: {str(e)}")


# Alternative version for raw file inputs (if your GeoJSON is stored as a raw file)
@transform(
    geojson_file=FileInput("ri.foundry.main.dataset.your-geojson-file-rid"),  # Raw file
    table_input=Input("ri.foundry.main.dataset.your-table-rid-here"),
    output_geojson=Output("ri.foundry.main.dataset.your-output-rid-here")
)
def merge_geojson_file_with_table(geojson_file, table_input, output_geojson):
    """
    Alternative version that handles GeoJSON stored as a raw file (not a dataset).
    Use this if your GeoJSON is uploaded as a file rather than imported as a dataset.
    """
    
    spark = table_input.spark_session
    
    try:
        # Read the raw GeoJSON file
        with geojson_file.open('r') as f:
            geojson_content = f.read()
        
        # Parse the GeoJSON
        geojson_data = json.loads(geojson_content)
        
        # Validate structure
        if 'features' not in geojson_data:
            if 'type' in geojson_data and geojson_data['type'] == 'Feature':
                geojson_data = {
                    "type": "FeatureCollection",
                    "features": [geojson_data]
                }
            else:
                raise ValueError("Invalid GeoJSON structure")
        
        features = geojson_data['features']
        
        # Extract properties
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        # Create DataFrame from properties
        geojson_props_df = spark.createDataFrame(properties_list)
        
        # Find county column
        county_col_geojson = None
        for col in geojson_props_df.columns:
            if col.upper() in ['COUNTY_NAME', 'COUNTYNAME', 'COUNTY', 'NAME']:
                county_col_geojson = col
                break
        
        if not county_col_geojson:
            raise ValueError(f"No county column found. Columns: {geojson_props_df.columns}")
        
        geojson_props_df = geojson_props_df.withColumn(
            'COUNTY_NAME_CLEAN',
            F.upper(F.trim(F.col(county_col_geojson)))
        )
        
        # Process table dataset
        table_df = table_input.dataframe()
        
        county_col_table = None
        for col in table_df.columns:
            if col.lower() in ['county', 'county_name', 'countyname']:
                county_col_table = col
                break
        
        if not county_col_table:
            raise ValueError(f"No county column in table. Columns: {table_df.columns}")
        
        table_df = table_df.withColumn(
            'county_clean',
            F.upper(F.trim(F.col(county_col_table)))
        )
        
        # Get columns to add from table
        table_columns = [col for col in table_df.columns 
                        if col not in [county_col_table, 'county_clean']]
        
        # Perform merge
        merged_df = geojson_props_df.join(
            table_df,
            geojson_props_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Collect results
        final_columns = ['_feature_index'] + \
                       [col for col in geojson_props_df.columns 
                        if col not in ['COUNTY_NAME_CLEAN', '_feature_index']] + \
                       table_columns
        
        merged_df = merged_df.select(*final_columns)
        merged_properties = merged_df.collect()
        
        # Map properties back
        properties_map = {}
        for row in merged_properties:
            row_dict = row.asDict()
            idx = row_dict.pop('_feature_index')
            cleaned = {k: v for k, v in row_dict.items() if v is not None}
            properties_map[idx] = cleaned
        
        # Rebuild features
        output_features = []
        for i, feature in enumerate(features):
            new_feature = {
                "type": feature.get("type", "Feature"),
                "geometry": feature.get("geometry", {}),
                "properties": properties_map.get(i, feature.get("properties", {}))
            }
            output_features.append(new_feature)
        
        # Create output GeoJSON
        output_data = {
            "type": "FeatureCollection",
            "features": output_features
        }
        
        # Save as dataset
        output_json = json.dumps(output_data, indent=2)
        output_df = spark.createDataFrame(
            [(output_json,)],
            schema=StructType([StructField("value", StringType(), True)])
        )
        
        output_geojson.write_dataframe(output_df)
        
        print(f"Merged {len(output_features)} features successfully")
        
    except Exception as e:
        raise ValueError(f"Error processing merge: {str(e)}")