import os

def load_weather_data(transformed_df):
    """
    Load the transformed weather data into the data/processed folder as a Parquet file using PySpark.

    Args:
        transformed_df (pyspark.sql.DataFrame): Transformed weather data as a PySpark DataFrame.
    """
    if transformed_df is None or transformed_df.rdd.isEmpty():
        print("No data to load. Exiting load step.")
        return

    # Ensure the data/processed folder exists
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)

    # Define the Parquet file path
    parquet_path = os.path.join(output_dir, "weather_data.parquet")

    try:
        # Write the DataFrame to a Parquet file in append mode
        transformed_df.write.mode("append").parquet(parquet_path)
        print(f"Transformed data successfully loaded to {parquet_path}.")
    except Exception as e:
        print(f"Error loading data to Parquet: {e}")