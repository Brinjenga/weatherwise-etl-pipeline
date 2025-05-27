from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def transform_weather_data(raw_data, spark):
    """
    Transform raw weather data into a PySpark DataFrame ready for loading.

    Args:
        raw_data (dict): Raw weather data from the OpenWeatherMap API.
        spark (SparkSession): Spark session for creating the DataFrame.

    Returns:
        pyspark.sql.DataFrame: Transformed weather data as a PySpark DataFrame.
    """
    if not raw_data:
        print("No data to transform.")
        return None

    try:
        # Define the schema for the transformed data
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("timestamp", LongType(), True)
        ])

        # Extract and transform relevant fields
        transformed_data = [{
            "city": raw_data.get("name"),
            "country": raw_data.get("sys", {}).get("country"),
            "temperature": raw_data.get("main", {}).get("temp"),
            "humidity": raw_data.get("main", {}).get("humidity"),
            "weather": raw_data.get("weather", [{}])[0].get("description"),
            "wind_speed": raw_data.get("wind", {}).get("speed"),
            "timestamp": raw_data.get("dt")  # Unix timestamp
        }]

        # Create a PySpark DataFrame from the transformed data
        df = spark.createDataFrame(transformed_data, schema=schema)

        print("Transformed data into PySpark DataFrame:")
        df.show()

        return df
    except Exception as e:
        print(f"Error transforming data: {e}")
        return None