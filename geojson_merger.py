from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import json


@transform(
    geojson_input=Input("/path/to/your/geojson/dataset"),  # Update with your GeoJSON dataset path
    table_input=Input("/path/to/your/table/dataset"),      # Update with your table dataset path
    output_geojson=Output("/path/to/output/geojson/dataset")  # Update with your output path
)
def merge_geojson_with_table(geojson_input, table_input, output_geojson):
    """
    Merges a GeoJSON file with a regular Foundry dataset based on county names.
    
    Args:
        geojson_input: Input GeoJSON file (unstructured)
        table_input: Input table dataset with county information
        output_geojson: Output merged GeoJSON file
    """
    
    # Get Spark context
    spark = geojson_input.spark_session
    
    # Step 1: Read the GeoJSON file as text
    geojson_rdd = geojson_input.dataframe().rdd
    
    # Collect the GeoJSON content (assuming it's a single file)
    # If multiple files, you may need to handle differently
    geojson_content = geojson_rdd.map(lambda row: row.value).collect()
    
    # Parse the GeoJSON content
    if len(geojson_content) == 1:
        # Single file case
        geojson_data = json.loads(geojson_content[0])
    else:
        # Multiple files - concatenate them
        merged_features = []
        for content in geojson_content:
            data = json.loads(content)
            if 'features' in data:
                merged_features.extend(data['features'])
        
        # Create a combined GeoJSON structure
        geojson_data = {
            "type": "FeatureCollection",
            "features": merged_features
        }
    
    # Step 2: Extract features and create a DataFrame from GeoJSON properties
    features = geojson_data.get('features', [])
    
    # Extract properties from each feature
    properties_list = []
    for i, feature in enumerate(features):
        properties = feature.get('properties', {})
        properties['_feature_index'] = i  # Add index to maintain feature order
        properties_list.append(properties)
    
    # Create DataFrame from properties
    if properties_list:
        geojson_df = spark.createDataFrame(properties_list)
        
        # Ensure COUNTY_NAME column exists and standardize it
        if 'COUNTY_NAME' in geojson_df.columns:
            # Standardize the county name for matching (trim spaces, uppercase)
            geojson_df = geojson_df.withColumn(
                'COUNTY_NAME_CLEAN', 
                F.upper(F.trim(F.col('COUNTY_NAME')))
            )
        else:
            raise ValueError("COUNTY_NAME column not found in GeoJSON properties")
    else:
        raise ValueError("No features found in GeoJSON file")
    
    # Step 3: Prepare the table dataset for joining
    table_df = table_input.dataframe()
    
    # Standardize the county column for matching
    if 'county' in table_df.columns:
        table_df = table_df.withColumn(
            'county_clean', 
            F.upper(F.trim(F.col('county')))
        )
    else:
        raise ValueError("county column not found in table dataset")
    
    # Get all columns from table dataset except the join column
    table_columns = [col for col in table_df.columns if col not in ['county', 'county_clean']]
    
    # Step 4: Perform the merge (left join to preserve all GeoJSON features)
    merged_df = geojson_df.join(
        table_df,
        geojson_df.COUNTY_NAME_CLEAN == table_df.county_clean,
        how='left'
    )
    
    # Select original GeoJSON columns plus new columns from table
    final_columns = geojson_df.columns + table_columns
    # Remove the temporary clean columns
    final_columns = [col for col in final_columns if col not in ['COUNTY_NAME_CLEAN', '_feature_index']]
    
    merged_df = merged_df.select('_feature_index', *final_columns)
    
    # Step 5: Convert back to GeoJSON format
    # Collect the merged data
    merged_properties = merged_df.collect()
    
    # Create a mapping of feature index to merged properties
    properties_map = {}
    for row in merged_properties:
        row_dict = row.asDict()
        feature_idx = row_dict.pop('_feature_index')
        properties_map[feature_idx] = row_dict
    
    # Update the original GeoJSON features with merged properties
    output_features = []
    for i, feature in enumerate(features):
        new_feature = feature.copy()
        if i in properties_map:
            # Update properties with merged data
            new_feature['properties'] = properties_map[i]
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
    
    # Step 6: Convert to JSON string and save as new file
    output_json_string = json.dumps(output_geojson_data, indent=2)
    
    # Create a DataFrame with the JSON string
    output_df = spark.createDataFrame(
        [(output_json_string,)], 
        schema=StructType([StructField("value", StringType(), True)])
    )
    
    # Write the output
    output_geojson.write_dataframe(output_df)


# Alternative implementation if you need to handle very large GeoJSON files more efficiently
@transform(
    geojson_input=Input("/path/to/your/geojson/dataset"),
    table_input=Input("/path/to/your/table/dataset"),
    output_geojson=Output("/path/to/output/geojson/dataset")
)
def merge_geojson_with_table_distributed(geojson_input, table_input, output_geojson):
    """
    Alternative implementation for very large GeoJSON files using distributed processing.
    """
    
    spark = geojson_input.spark_session
    
    # Read GeoJSON as text
    geojson_text_df = geojson_input.dataframe()
    
    # Parse GeoJSON using Spark's built-in JSON capabilities
    # This assumes each line is a valid JSON object (for line-delimited GeoJSON)
    from pyspark.sql.functions import from_json, col, explode
    from pyspark.sql.types import MapType, StringType, ArrayType
    
    # If the GeoJSON is a single document, we need to handle it differently
    # First, try to parse as a complete document
    geojson_content = geojson_text_df.select("value").first()[0]
    geojson_data = json.loads(geojson_content)
    
    # Convert features to DataFrame
    features_rdd = spark.sparkContext.parallelize(geojson_data['features'])
    
    def extract_properties_with_geometry(feature):
        """Extract properties and maintain geometry reference"""
        properties = feature.get('properties', {})
        properties['_geometry'] = json.dumps(feature.get('geometry', {}))
        properties['_type'] = feature.get('type', 'Feature')
        return properties
    
    # Create DataFrame from features
    properties_rdd = features_rdd.map(extract_properties_with_geometry)
    geojson_df = spark.createDataFrame(properties_rdd)
    
    # Standardize county name
    geojson_df = geojson_df.withColumn(
        'COUNTY_NAME_CLEAN',
        F.upper(F.trim(F.col('COUNTY_NAME')))
    )
    
    # Prepare table dataset
    table_df = table_input.dataframe()
    table_df = table_df.withColumn(
        'county_clean',
        F.upper(F.trim(F.col('county')))
    )
    
    # Perform merge
    merged_df = geojson_df.join(
        table_df,
        geojson_df.COUNTY_NAME_CLEAN == table_df.county_clean,
        how='left'
    )
    
    # Reconstruct GeoJSON
    def reconstruct_feature(row):
        """Reconstruct GeoJSON feature from row"""
        row_dict = row.asDict()
        
        # Extract special fields
        geometry = json.loads(row_dict.pop('_geometry', '{}'))
        feature_type = row_dict.pop('_type', 'Feature')
        
        # Remove temporary columns
        row_dict.pop('COUNTY_NAME_CLEAN', None)
        row_dict.pop('county_clean', None)
        row_dict.pop('county', None)
        
        # Build feature
        feature = {
            "type": feature_type,
            "geometry": geometry,
            "properties": {k: v for k, v in row_dict.items() if v is not None}
        }
        
        return feature
    
    # Collect and reconstruct features
    merged_features = merged_df.rdd.map(reconstruct_feature).collect()
    
    # Create output GeoJSON
    output_geojson_data = {
        "type": "FeatureCollection",
        "features": merged_features
    }
    
    # Save as new file
    output_json_string = json.dumps(output_geojson_data, indent=2)
    output_df = spark.createDataFrame(
        [(output_json_string,)],
        schema=StructType([StructField("value", StringType(), True)])
    )
    
    output_geojson.write_dataframe(output_df)
