from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

def save_raw_data_to_csv(city_name, raw_data):
    """
    Save raw weather data to a CSV file using Spark.

    Args:
        city_name (str): Name of the city.
        raw_data (dict): Raw weather data in JSON format.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WeatherDataETL") \
        .getOrCreate()

    # Ensure the data/raw folder exists
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Define the schema for the raw data
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("weather", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # Extract relevant fields for the CSV file
    row = [{
        "city": city_name,
        "country": raw_data.get("sys", {}).get("country"),
        "temperature": raw_data.get("main", {}).get("temp"),
        "humidity": raw_data.get("main", {}).get("humidity"),
        "weather": raw_data.get("weather", [{}])[0].get("description"),
        "wind_speed": raw_data.get("wind", {}).get("speed"),
        "timestamp": raw_data.get("dt")  # Unix timestamp
    }]

    # Create a PySpark DataFrame from the row
    df = spark.createDataFrame(row, schema=schema)

    # Define the CSV file path
    csv_path = os.path.join(output_dir, "weather_data.csv")

    # Write the DataFrame to a CSV file (overwrite mode for simplicity)
    df.write.mode("append").option("header", "true").csv(csv_path)

    print(f"Raw weather data for {city_name} saved to {csv_path}.")