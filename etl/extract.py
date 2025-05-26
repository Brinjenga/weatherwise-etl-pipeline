import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def get_weather_data(city_name, api_key):
    """
    Fetch weather data for a given city from the OpenWeatherMap API and save it as a Parquet file using Spark.

    Args:
        city_name (str): Name of the city to fetch weather data for.
        api_key (str): API key for OpenWeatherMap.

    Returns:
        dict: Weather data in JSON format if successful, None otherwise.
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "imperial"  # Use "metric" for Celsius
    }

    try:
        # Fetch weather data from the API
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        raw_data = response.json()

        # Save the raw data to a Parquet file using Spark
        save_raw_data_to_parquet(city_name, raw_data)

        return raw_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def save_raw_data_to_parquet(city_name, raw_data):
    """
    Save raw weather data to a Parquet file using Spark.

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

    # Define the schema for the weather data
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("weather", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # Extract relevant fields for the Parquet file
    row = {
        "city": city_name,
        "country": raw_data.get("sys", {}).get("country"),
        "temperature": raw_data.get("main", {}).get("temp"),
        "humidity": raw_data.get("main", {}).get("humidity"),
        "weather": raw_data.get("weather", [{}])[0].get("description"),
        "wind_speed": raw_data.get("wind", {}).get("speed"),
        "timestamp": raw_data.get("dt")  # Unix timestamp
    }

    # Create a DataFrame from the row
    df = spark.createDataFrame([row], schema=schema)

    # Write the DataFrame to a Parquet file (append mode)
    parquet_path = os.path.join(output_dir, "weather_data.parquet")
    df.write.mode("append").parquet(parquet_path)

    print(f"Raw weather data for {city_name} saved to {parquet_path}.")