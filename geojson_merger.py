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
    
    This version handles GeoJSON stored as raw files or unstructured datasets.
    """
    
    try:
        # Step 1: Try to read GeoJSON - handle both structured and raw file formats
        print("Attempting to read GeoJSON dataset...")
        
        geojson_data = None
        spark = None
        
        try:
            # First attempt: Try reading as a structured dataset
            geojson_df = geojson_input.dataframe()
            spark = geojson_df.sparkSession
            
            # Check if the dataframe has content
            if geojson_df.count() == 0:
                raise ValueError("GeoJSON dataset is empty")
            
            print("Successfully read as structured dataset")
            print("Column names:", geojson_df.columns)
            
            # Handle different column formats
            if 'value' in geojson_df.columns:
                geojson_content = geojson_df.select("value").collect()
                if len(geojson_content) == 1:
                    geojson_data = json.loads(geojson_content[0][0])
                else:
                    merged_features = []
                    for row in geojson_content:
                        data = json.loads(row[0])
                        if 'features' in data:
                            merged_features.extend(data['features'])
                    geojson_data = {"type": "FeatureCollection", "features": merged_features}
                    
            elif 'content' in geojson_df.columns:
                geojson_content = geojson_df.select("content").first()[0]
                geojson_data = json.loads(geojson_content)
                
            else:
                # Try first column
                first_col = geojson_df.columns[0]
                content = geojson_df.select(first_col).first()[0]
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
                geojson_data = json.loads(content)
                
        except Exception as e:
            print(f"Could not read as structured dataset: {str(e)}")
            print("Attempting to read as raw file...")
            
            # Second attempt: Read as raw file using filesystem
            try:
                # Get the filesystem and read the file
                filesystem = geojson_input.filesystem()
                files = filesystem.ls()
                
                print(f"Found {len(files)} files in dataset")
                
                # Look for .geojson or .json files
                json_files = [f for f in files if f.path.endswith(('.geojson', '.json'))]
                
                if not json_files:
                    # If no json files, try reading the first file
                    if files:
                        json_files = [files[0]]
                    else:
                        raise ValueError("No files found in dataset")
                
                print(f"Reading file: {json_files[0].path}")
                
                # Read the file content
                with filesystem.open(json_files[0].path, 'r') as f:
                    geojson_content = f.read()
                    geojson_data = json.loads(geojson_content)
                
                # Get Spark session from table input since we couldn't get it from geojson
                table_df_temp = table_input.dataframe()
                spark = table_df_temp.sparkSession
                
            except Exception as e2:
                print(f"Could not read as raw file: {str(e2)}")
                
                # Third attempt: Try reading with .dataframe() but handle as bytes
                try:
                    geojson_df = geojson_input.dataframe()
                    spark = geojson_df.sparkSession
                    
                    # Collect all data and try to decode
                    all_data = geojson_df.collect()
                    
                    for row in all_data:
                        for value in row:
                            if value:
                                try:
                                    if isinstance(value, bytes):
                                        content = value.decode('utf-8')
                                    else:
                                        content = str(value)
                                    geojson_data = json.loads(content)
                                    break
                                except:
                                    continue
                        if geojson_data:
                            break
                            
                except Exception as e3:
                    raise ValueError(f"Unable to read GeoJSON dataset in any format. Errors: {str(e)}, {str(e2)}, {str(e3)}")
        
        if not geojson_data:
            raise ValueError("Failed to parse GeoJSON data from dataset")
        
        if not spark:
            # Get Spark session from table input as fallback
            table_df_temp = table_input.dataframe()
            spark = table_df_temp.sparkSession
        
        print("Successfully loaded GeoJSON data")
        
        # Validate GeoJSON structure
        if 'features' not in geojson_data:
            if 'type' in geojson_data and geojson_data['type'] == 'Feature':
                geojson_data = {"type": "FeatureCollection", "features": [geojson_data]}
            else:
                raise ValueError("Invalid GeoJSON structure - no features found")
        
        # Step 2: Extract features and create DataFrame from properties
        features = geojson_data.get('features', [])
        
        if not features:
            raise ValueError("No features found in GeoJSON file")
        
        print(f"Found {len(features)} features in GeoJSON")
        
        # Extract properties
        properties_list = []
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        # Create DataFrame from properties
        if not properties_list or not any(properties_list):
            raise ValueError("No properties found in GeoJSON features")
            
        geojson_props_df = spark.createDataFrame(properties_list)
        
        print("GeoJSON Properties DataFrame Schema:")
        geojson_props_df.printSchema()
        
        # Find county column
        county_col_geojson = None
        for col in geojson_props_df.columns:
            if col.upper() in ['COUNTY_NAME', 'COUNTYNAME', 'COUNTY', 'NAME']:
                county_col_geojson = col
                print(f"Found county column in GeoJSON: {col}")
                break
        
        if not county_col_geojson:
            print(f"Available columns in GeoJSON: {geojson_props_df.columns}")
            raise ValueError(f"No county name column found in GeoJSON properties")
        
        # Standardize county name
        geojson_props_df = geojson_props_df.withColumn(
            'COUNTY_NAME_CLEAN', 
            F.upper(F.trim(F.col(county_col_geojson)))
        )
        
        print("Sample county names from GeoJSON:")
        geojson_props_df.select(county_col_geojson, 'COUNTY_NAME_CLEAN').show(5, truncate=False)
        
    except Exception as e:
        print(f"Error processing GeoJSON: {str(e)}")
        raise ValueError(f"Error processing GeoJSON input: {str(e)}")
    
    try:
        # Step 3: Prepare table dataset
        table_df = table_input.dataframe()
        
        print("\nTable DataFrame Schema:")
        table_df.printSchema()
        print(f"Number of rows in table: {table_df.count()}")
        
        if table_df.count() == 0:
            raise ValueError("Table dataset is empty")
        
        # Find county column in table
        county_col_table = None
        for col in table_df.columns:
            if col.lower() in ['county', 'county_name', 'countyname', 'name']:
                county_col_table = col
                print(f"Found county column in table: {col}")
                break
        
        if not county_col_table:
            print(f"Available columns in table: {table_df.columns}")
            raise ValueError(f"No county column found in table dataset")
        
        # Standardize county column
        table_df = table_df.withColumn(
            'county_clean', 
            F.upper(F.trim(F.col(county_col_table)))
        )
        
        print("Sample county names from table:")
        table_df.select(county_col_table, 'county_clean').show(5, truncate=False)
        
        # Get columns to add from table
        table_columns = [col for col in table_df.columns 
                        if col not in [county_col_table, 'county_clean']]
        
        print(f"Columns to be added from table: {table_columns}")
        
    except Exception as e:
        print(f"Error processing table: {str(e)}")
        raise ValueError(f"Error processing table input: {str(e)}")
    
    try:
        # Step 4: Perform merge
        print("\nPerforming join operation...")
        merged_df = geojson_props_df.join(
            table_df,
            geojson_props_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Log statistics
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
        
        # Select final columns
        final_columns = ['_feature_index'] + \
                       [col for col in geojson_props_df.columns 
                        if col not in ['COUNTY_NAME_CLEAN', '_feature_index']] + \
                       table_columns
        
        merged_df = merged_df.select(*final_columns)
        
        # Step 5: Convert back to GeoJSON
        print("\nConverting back to GeoJSON format...")
        
        merged_properties = merged_df.collect()
        
        # Create mapping
        properties_map = {}
        for row in merged_properties:
            row_dict = row.asDict()
            feature_idx = row_dict.pop('_feature_index')
            cleaned_dict = {k: v for k, v in row_dict.items() if v is not None}
            properties_map[feature_idx] = cleaned_dict
        
        # Update features
        output_features = []
        for i, feature in enumerate(features):
            new_feature = {
                "type": feature.get("type", "Feature"),
                "geometry": feature.get("geometry", {})
            }
            
            if i in properties_map:
                new_feature["properties"] = properties_map[i]
            else:
                new_feature["properties"] = feature.get("properties", {})
            
            # Preserve other attributes
            for key, value in feature.items():
                if key not in ["type", "geometry", "properties"]:
                    new_feature[key] = value
            
            output_features.append(new_feature)
        
        # Create output GeoJSON
        output_geojson_data = {
            "type": geojson_data.get("type", "FeatureCollection"),
            "features": output_features
        }
        
        # Add other top-level properties
        for key, value in geojson_data.items():
            if key not in ["type", "features"]:
                output_geojson_data[key] = value
        
        # Step 6: Save output
        output_json_string = json.dumps(output_geojson_data, indent=2)
        
        # Create output DataFrame
        output_df = spark.createDataFrame(
            [(output_json_string,)], 
            schema=StructType([StructField("value", StringType(), True)])
        )
        
        # Write output
        output_geojson.write_dataframe(output_df)
        
        print(f"\n✅ Successfully merged GeoJSON with table data.")
        print(f"   Output contains {len(output_features)} features.")
        print(f"   {matched_features} features were enriched with table data.")
        
    except Exception as e:
        print(f"Error during merge: {str(e)}")
        raise ValueError(f"Error during merge operation: {str(e)}")