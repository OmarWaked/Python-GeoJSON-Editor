from transforms.api import transform, Input, Output
from transforms.api import incremental
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
    
    This version handles GeoJSON stored as raw files using Foundry's filesystem API.
    """
    
    # Get Spark session from table input first (since it's more reliable)
    table_df = table_input.dataframe()
    spark = table_df.sparkSession
    
    print("Starting GeoJSON merge process...")
    
    try:
        # Step 1: Read GeoJSON using multiple strategies
        print("Attempting to read GeoJSON dataset...")
        geojson_data = None
        
        # Strategy 1: Try using filesystem() to read raw files
        try:
            print("Strategy 1: Attempting filesystem access...")
            fs = geojson_input.filesystem()
            
            # List all files
            all_files = list(fs.ls())
            print(f"Found {len(all_files)} files in dataset")
            
            if not all_files:
                raise ValueError("No files found in dataset")
            
            # Look for JSON/GeoJSON files
            json_files = [f for f in all_files if str(f.path).endswith(('.geojson', '.json', '.JSON', '.GEOJSON'))]
            
            if not json_files:
                # Try any file that might be JSON
                print("No .json/.geojson files found, trying all files...")
                json_files = all_files
            
            # Try to read each file until we find valid GeoJSON
            for file_info in json_files:
                try:
                    print(f"Trying to read file: {file_info.path}")
                    with fs.open(file_info.path, 'rb') as f:
                        content = f.read()
                        
                    # Try to decode and parse
                    if isinstance(content, bytes):
                        content = content.decode('utf-8')
                    
                    geojson_data = json.loads(content)
                    print(f"Successfully parsed GeoJSON from {file_info.path}")
                    break
                    
                except Exception as e:
                    print(f"Failed to read {file_info.path}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Strategy 1 failed: {str(e)}")
        
        # Strategy 2: Try reading as Spark DataFrame with different approaches
        if not geojson_data:
            try:
                print("Strategy 2: Attempting DataFrame access...")
                
                # Try reading with text format
                try:
                    print("Trying to read as text format...")
                    df = spark.read.text(geojson_input.path)
                    
                    # Collect all lines and join them
                    lines = df.collect()
                    full_content = "".join([row.value for row in lines])
                    geojson_data = json.loads(full_content)
                    print("Successfully read as text format")
                    
                except Exception as e2:
                    print(f"Text format failed: {str(e2)}")
                    
                    # Try reading with json format
                    try:
                        print("Trying to read as JSON format...")
                        df = spark.read.json(geojson_input.path)
                        
                        # Convert DataFrame to dictionary
                        # This handles cases where Spark auto-parses the JSON
                        rows = df.collect()
                        
                        if df.columns == ['features', 'type'] or 'features' in df.columns:
                            # It's already parsed as GeoJSON structure
                            first_row = rows[0]
                            geojson_data = first_row.asDict()
                        else:
                            # Try to reconstruct from rows
                            features = []
                            for row in rows:
                                features.append(row.asDict())
                            geojson_data = {"type": "FeatureCollection", "features": features}
                            
                        print("Successfully read as JSON format")
                        
                    except Exception as e3:
                        print(f"JSON format failed: {str(e3)}")
                        
            except Exception as e:
                print(f"Strategy 2 failed: {str(e)}")
        
        # Strategy 3: Try using dataframe() with various column handling
        if not geojson_data:
            try:
                print("Strategy 3: Attempting standard dataframe access...")
                geojson_df = geojson_input.dataframe()
                
                print(f"DataFrame has {geojson_df.count()} rows")
                print(f"Columns: {geojson_df.columns}")
                
                # Show schema for debugging
                geojson_df.printSchema()
                
                # Try different column names
                possible_columns = ['value', 'content', 'text', 'data', 'json', '_c0']
                
                for col_name in possible_columns + geojson_df.columns:
                    if col_name in geojson_df.columns:
                        try:
                            print(f"Trying column: {col_name}")
                            
                            # Get first non-null value
                            first_row = geojson_df.filter(F.col(col_name).isNotNull()).first()
                            
                            if first_row:
                                content = first_row[col_name]
                                
                                if isinstance(content, bytes):
                                    content = content.decode('utf-8')
                                elif not isinstance(content, str):
                                    content = str(content)
                                
                                geojson_data = json.loads(content)
                                print(f"Successfully parsed from column {col_name}")
                                break
                                
                        except Exception as e:
                            print(f"Failed to parse column {col_name}: {str(e)}")
                            continue
                            
            except Exception as e:
                print(f"Strategy 3 failed: {str(e)}")
        
        # If still no data, try one more approach - reading as binary file
        if not geojson_data:
            try:
                print("Strategy 4: Attempting binary file access...")
                # This approach works for datasets that are stored as binary files
                fs = geojson_input.filesystem()
                
                # Get the first file regardless of extension
                files = list(fs.ls())
                if files:
                    with fs.open(files[0].path, 'rb') as f:
                        content = f.read()
                    
                    # Try different encodings
                    for encoding in ['utf-8', 'utf-16', 'latin-1']:
                        try:
                            decoded = content.decode(encoding)
                            geojson_data = json.loads(decoded)
                            print(f"Successfully decoded with {encoding}")
                            break
                        except:
                            continue
                            
            except Exception as e:
                print(f"Strategy 4 failed: {str(e)}")
        
        if not geojson_data:
            # Provide helpful error message
            error_msg = """
            Failed to read GeoJSON dataset. Please ensure:
            1. The dataset contains a valid GeoJSON file
            2. The file is properly formatted JSON
            3. The dataset has been properly ingested into Foundry
            
            You may need to:
            - Re-upload the GeoJSON file
            - Use Foundry's data import wizard to properly ingest the file
            - Ensure the file is saved as UTF-8 encoded text
            """
            raise ValueError(error_msg)
        
        print("Successfully loaded GeoJSON data")
        print(f"GeoJSON type: {geojson_data.get('type', 'Unknown')}")
        
        # Validate and normalize GeoJSON structure
        if 'features' not in geojson_data:
            if 'type' in geojson_data and geojson_data['type'] == 'Feature':
                geojson_data = {"type": "FeatureCollection", "features": [geojson_data]}
            elif isinstance(geojson_data, list):
                # Might be a list of features
                geojson_data = {"type": "FeatureCollection", "features": geojson_data}
            else:
                raise ValueError(f"Invalid GeoJSON structure. Keys found: {list(geojson_data.keys())}")
        
        # Extract features
        features = geojson_data.get('features', [])
        
        if not features:
            raise ValueError("No features found in GeoJSON file")
        
        print(f"Found {len(features)} features in GeoJSON")
        
        # Extract properties from features
        properties_list = []
        for i, feature in enumerate(features):
            if not isinstance(feature, dict):
                print(f"Warning: Feature {i} is not a dictionary, skipping...")
                continue
                
            properties = feature.get('properties', {})
            if properties is None:
                properties = {}
            
            properties['_feature_index'] = i
            properties_list.append(properties)
        
        if not properties_list:
            raise ValueError("No valid properties found in GeoJSON features")
        
        # Create DataFrame from properties
        geojson_props_df = spark.createDataFrame(properties_list)
        
        print("GeoJSON Properties DataFrame created successfully")
        print("Schema:")
        geojson_props_df.printSchema()
        print(f"Number of rows: {geojson_props_df.count()}")
        print(f"Columns: {geojson_props_df.columns}")
        
        # Find county column (case-insensitive)
        county_col_geojson = None
        for col in geojson_props_df.columns:
            col_upper = col.upper()
            if any(term in col_upper for term in ['COUNTY', 'NAME', 'COUNTY_NAME', 'COUNTYNAME']):
                county_col_geojson = col
                print(f"Found county column in GeoJSON: {col}")
                break
        
        if not county_col_geojson:
            # Show available columns for debugging
            print("Available columns in GeoJSON properties:")
            for col in geojson_props_df.columns:
                if col != '_feature_index':
                    sample_values = geojson_props_df.select(col).limit(3).collect()
                    print(f"  - {col}: {[row[0] for row in sample_values]}")
            
            raise ValueError("No county name column found. Please check the column names above.")
        
        # Standardize county name for matching
        geojson_props_df = geojson_props_df.withColumn(
            'COUNTY_NAME_CLEAN', 
            F.upper(F.trim(F.col(county_col_geojson)))
        )
        
        print("\nSample county names from GeoJSON:")
        geojson_props_df.select(county_col_geojson, 'COUNTY_NAME_CLEAN').show(5, truncate=False)
        
    except Exception as e:
        print(f"Error processing GeoJSON: {str(e)}")
        raise ValueError(f"Error processing GeoJSON input: {str(e)}")
    
    try:
        # Step 2: Process table dataset
        print("\n" + "="*50)
        print("Processing table dataset...")
        
        print("Table DataFrame Schema:")
        table_df.printSchema()
        print(f"Number of rows in table: {table_df.count()}")
        
        if table_df.count() == 0:
            raise ValueError("Table dataset is empty")
        
        # Find county column in table
        county_col_table = None
        for col in table_df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['county', 'county_name', 'countyname']):
                county_col_table = col
                print(f"Found county column in table: {col}")
                break
        
        if not county_col_table:
            print("Available columns in table:")
            for col in table_df.columns:
                print(f"  - {col}")
            raise ValueError("No county column found in table dataset")
        
        # Standardize county column for matching
        table_df = table_df.withColumn(
            'county_clean', 
            F.upper(F.trim(F.col(county_col_table)))
        )
        
        print("\nSample county names from table:")
        table_df.select(county_col_table, 'county_clean').show(5, truncate=False)
        
        # Get columns to add from table
        table_columns = [col for col in table_df.columns 
                        if col not in [county_col_table, 'county_clean']]
        
        print(f"\nColumns to be added from table: {table_columns}")
        
    except Exception as e:
        print(f"Error processing table: {str(e)}")
        raise ValueError(f"Error processing table input: {str(e)}")
    
    try:
        # Step 3: Perform the merge
        print("\n" + "="*50)
        print("Performing join operation...")
        
        merged_df = geojson_props_df.join(
            table_df,
            geojson_props_df.COUNTY_NAME_CLEAN == table_df.county_clean,
            how='left'
        )
        
        # Calculate statistics
        total_features = geojson_props_df.count()
        matched_features = merged_df.filter(F.col('county_clean').isNotNull()).count()
        unmatched_features = total_features - matched_features
        
        print(f"\nJoin statistics:")
        print(f"  Total features: {total_features}")
        print(f"  Matched features: {matched_features}")
        print(f"  Unmatched features: {unmatched_features}")
        
        if unmatched_features > 0:
            print("\nUnmatched counties from GeoJSON (first 20):")
            unmatched = merged_df.filter(F.col('county_clean').isNull()).select(county_col_geojson).distinct()
            unmatched.show(20, truncate=False)
        
        # Select final columns
        final_columns = ['_feature_index'] + \
                       [col for col in geojson_props_df.columns 
                        if col not in ['COUNTY_NAME_CLEAN', '_feature_index']] + \
                       table_columns
        
        merged_df = merged_df.select(*final_columns)
        
        # Step 4: Reconstruct GeoJSON with merged properties
        print("\n" + "="*50)
        print("Converting back to GeoJSON format...")
        
        # Collect merged properties
        merged_properties = merged_df.collect()
        
        # Create mapping of feature index to properties
        properties_map = {}
        for row in merged_properties:
            row_dict = row.asDict()
            feature_idx = row_dict.pop('_feature_index')
            # Remove None values
            cleaned_dict = {k: v for k, v in row_dict.items() if v is not None}
            properties_map[feature_idx] = cleaned_dict
        
        # Update features with merged properties
        output_features = []
        for i, feature in enumerate(features):
            new_feature = {
                "type": feature.get("type", "Feature"),
                "geometry": feature.get("geometry", {})
            }
            
            # Use merged properties if available
            if i in properties_map:
                new_feature["properties"] = properties_map[i]
            else:
                new_feature["properties"] = feature.get("properties", {})
            
            # Preserve other feature attributes
            for key, value in feature.items():
                if key not in ["type", "geometry", "properties"]:
                    new_feature[key] = value
            
            output_features.append(new_feature)
        
        # Create output GeoJSON structure
        output_geojson_data = {
            "type": geojson_data.get("type", "FeatureCollection"),
            "features": output_features
        }
        
        # Preserve other top-level properties
        for key, value in geojson_data.items():
            if key not in ["type", "features"]:
                output_geojson_data[key] = value
        
        # Step 5: Write output
        print("\nWriting output GeoJSON...")
        output_json_string = json.dumps(output_geojson_data, indent=2)
        
        # Create output DataFrame
        output_df = spark.createDataFrame(
            [(output_json_string,)], 
            schema=StructType([StructField("value", StringType(), True)])
        )
        
        # Write to output dataset
        output_geojson.write_dataframe(output_df)
        
        print("\n" + "="*50)
        print(f"✅ Successfully merged GeoJSON with table data!")
        print(f"   Output contains {len(output_features)} features")
        print(f"   {matched_features} features were enriched with table data")
        print(f"   {unmatched_features} features had no matching county in table")
        
    except Exception as e:
        print(f"Error during merge operation: {str(e)}")
        raise ValueError(f"Error during merge operation: {str(e)}")