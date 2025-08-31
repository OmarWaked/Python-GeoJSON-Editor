# GeoJSON Merger for Foundry

A Python-based Foundry transform that merges GeoJSON files with regular table datasets based on county names. This tool is designed to enrich geographic data with additional attributes from structured datasets.

## 🚀 Features

- **GeoJSON Processing**: Handles both single and multiple GeoJSON files
- **County Name Matching**: Intelligent county name standardization for accurate joins
- **Data Enrichment**: Merges table data with GeoJSON properties while preserving geometry
- **Two Implementations**: 
  - Standard implementation for typical use cases
  - Distributed implementation for large-scale processing
- **Error Handling**: Comprehensive validation and error handling
- **Testing Framework**: Full test suite with mock implementations for local development

## 📋 Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Foundry environment (for production use)
- Access to GeoJSON and table datasets

## 🛠️ Installation

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd Python_GeoJSON_Editor
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```bash
   python -c "import pyspark; print('PySpark installed successfully')"
   ```

## 🏗️ Project Structure

```
Python_GeoJSON_Editor/
├── geojson_merger.py          # Main transformation functions
├── test_geojson_merger.py     # Comprehensive test suite
├── mock_transforms.py          # Mock Foundry API for local testing
├── run_local_tests.py          # Local test runner script
├── requirements.txt            # Python dependencies
├── pytest.ini                 # Test configuration
├── README.md                  # This file
├── sample_geojson.json        # Sample GeoJSON data (created by test runner)
└── sample_table.json          # Sample table data (created by test runner)
```

## 🔧 Configuration

### Foundry Paths

Update the dataset paths in `geojson_merger.py`:

```python
@transform(
    geojson_input=Input("/your/actual/geojson/dataset"),      # Update this
    table_input=Input("/your/actual/table/dataset"),          # Update this
    output_geojson=Output("/your/actual/output/dataset")      # Update this
)
```

### Data Requirements

**GeoJSON Input:**
- Must contain a `COUNTY_NAME` property in each feature
- Supports both single files and multiple file collections
- Geometry types: Point, LineString, Polygon, MultiPolygon, etc.

**Table Input:**
- Must contain a `county` column for matching
- Additional columns will be merged into the output GeoJSON

## 🧪 Testing

### Local Testing

1. **Run the local test runner:**
   ```bash
   python run_local_tests.py
   ```

2. **Run unit tests:**
   ```bash
   python -m pytest test_geojson_merger.py -v
   ```

3. **Run with coverage:**
   ```bash
   python -m pytest --cov=geojson_merger --cov-report=html
   ```

### Test Coverage

The test suite covers:
- ✅ GeoJSON parsing and validation
- ✅ County name standardization
- ✅ DataFrame operations and joins
- ✅ GeoJSON reconstruction
- ✅ Error handling scenarios
- ✅ Both standard and distributed implementations

## 📊 Usage Examples

### Basic Usage

```python
from geojson_merger import merge_geojson_with_table

# The function will be called automatically by Foundry
# when the transform is executed
```

### Expected Output

The transform produces enriched GeoJSON with:
- Original geometry preserved
- Original properties maintained
- New columns from the table dataset merged in
- County-based matching using standardized names

## 🔍 How It Works

1. **Input Processing**: Reads GeoJSON and table datasets
2. **County Standardization**: Normalizes county names for matching
3. **Data Join**: Performs left join on county names
4. **Feature Reconstruction**: Rebuilds GeoJSON with enriched properties
5. **Output Generation**: Writes merged GeoJSON to output dataset

## 🚨 Error Handling

The transform handles various error conditions:
- Missing `COUNTY_NAME` column in GeoJSON
- Missing `county` column in table data
- Empty GeoJSON features
- Invalid JSON structure
- Join failures

## 🔧 Customization

### Adding New Join Fields

To join on additional fields beyond county names:

```python
# Modify the join condition in the functions
merged_df = geojson_df.join(
    table_df,
    (geojson_df.COUNTY_NAME_CLEAN == table_df.county_clean) &
    (geojson_df.STATE_NAME == table_df.state_name),  # Add this
    how='left'
)
```

### Custom Data Transformations

Add data cleaning or transformation logic:

```python
# In the table preparation section
table_df = table_df.withColumn(
    'custom_field',
    F.upper(F.col('original_field'))
)
```

## 📈 Performance Considerations

- **Standard Implementation**: Best for datasets under 1GB
- **Distributed Implementation**: Optimized for large-scale processing
- **Memory Usage**: Monitor Spark executor memory for large GeoJSON files
- **Partitioning**: Consider partitioning large datasets by county for better performance

## 🐛 Troubleshooting

### Common Issues

1. **County Name Mismatches**
   - Check for typos, extra spaces, or case differences
   - Verify the standardization logic handles your data format

2. **Memory Issues**
   - Increase Spark executor memory
   - Use the distributed implementation for large files

3. **Join Failures**
   - Verify both datasets have the expected column names
   - Check data types and null values

### Debug Mode

Enable detailed logging by setting Spark log level:

```python
spark.sparkContext.setLogLevel("DEBUG")
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
- Check the test suite for usage examples
- Review error messages for troubleshooting hints
- Ensure your data meets the required format specifications

## 🔮 Future Enhancements

- Support for additional geometry types
- Configurable join strategies
- Performance optimization for very large datasets
- Additional data validation rules
- Support for nested GeoJSON properties
