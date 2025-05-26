import yaml
from pyspark.sql import SparkSession
from etl.extract import get_weather_data
from etl.transform import transform_weather_data  
from etl.load import load_weather_data          

def load_config(config_file):
    """
    Load configuration from a YAML file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: Configuration data.
    """
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Configuration file {config_file} not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None

def main():
    """
    Main function to orchestrate the ETL pipeline.
    """
    # Load configuration
    config = load_config("config.yaml")
    if config is None:
        print("Failed to load configuration. Exiting pipeline.")
        return

    # Extract API key and city name from the configuration
    api_key = config.get("apiKey")
    city_name = "New York"

    if not api_key:
        print("API key is missing in the configuration. Exiting pipeline.")
        return

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WeatherDataETL") \
        .getOrCreate()

    # Step 1: Extract
    raw_data = get_weather_data(city_name, api_key, spark)
    if raw_data is None:
        print("Failed to fetch weather data. Exiting pipeline.")
        return

    # Step 2: Transform
    transformed_data = transform_weather_data(raw_data)

    # Step 3: Load
    load_weather_data(transformed_data)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()