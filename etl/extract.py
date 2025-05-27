from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import requests
import os

def get_weather_data(city_name, api_key):
    """
    Fetch weather data for a given city from the OpenWeatherMap API and save it to a CSV file.

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

        # Save the raw data to a CSV file
        save_raw_data_to_csv(city_name, raw_data)

        return raw_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

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
        StructField("temperature", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("weather", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("timestamp", StringType(), True)
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