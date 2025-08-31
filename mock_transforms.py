"""
Mock implementation of the transforms API for local testing.
This allows the GeoJSON merger code to run locally without Foundry.
"""

import json
from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


class MockInput:
    """Mock implementation of Input for testing."""
    
    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        self.path = path
        self._spark_session = spark_session or SparkSession.builder \
            .appName("MockTransform") \
            .master("local[2]") \
            .getOrCreate()
    
    @property
    def spark_session(self) -> SparkSession:
        """Return the Spark session."""
        return self._spark_session
    
    def dataframe(self) -> DataFrame:
        """Return a mock DataFrame based on the path."""
        # For testing, create sample data based on path
        if "geojson" in self.path.lower():
            # Return sample GeoJSON data
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
            
            return self._spark_session.createDataFrame(
                [(json.dumps(sample_geojson),)],
                schema=StructType([StructField("value", StringType(), True)])
            )
        
        elif "table" in self.path.lower():
            # Return sample table data
            sample_table_data = [
                {"county": "Los Angeles", "crime_rate": 0.05, "unemployment": 0.08},
                {"county": "Orange County", "crime_rate": 0.03, "unemployment": 0.06},
                {"county": "San Diego", "crime_rate": 0.04, "unemployment": 0.07}
            ]
            
            return self._spark_session.createDataFrame(sample_table_data)
        
        else:
            # Default empty DataFrame
            return self._spark_session.createDataFrame([], StructType([]))


class MockOutput:
    """Mock implementation of Output for testing."""
    
    def __init__(self, path: str):
        self.path = path
        self._dataframe: Optional[DataFrame] = None
    
    def write_dataframe(self, df: DataFrame) -> None:
        """Mock writing a DataFrame."""
        self._dataframe = df
        print(f"Mock write to {self.path}: {df.count()} rows")
    
    def get_written_dataframe(self) -> Optional[DataFrame]:
        """Get the DataFrame that was written (for testing)."""
        return self._dataframe


def transform(*args, **kwargs):
    """Mock transform decorator that does nothing."""
    def decorator(func):
        return func
    return decorator


# Mock the actual API classes
Input = MockInput
Output = MockOutput
