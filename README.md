# WeatherWise ETL Pipeline

WeatherWise is an ETL (Extract, Transform, Load) pipeline designed to fetch, process, and store weather data from the [OpenWeatherMap API](https://openweathermap.org/api). The pipeline is built using Python and Apache Spark, ensuring scalability and efficiency for handling weather data for multiple cities.

## Features

- **Extraction**: 
Fetches real-time weather data for specified cities using the OpenWeatherMap API.

- **Transformation**: 
Cleans and structures the raw weather data into a format suitable for analysis.

- **Loading**: 
Stores the processed data in Parquet format using Apache Spark for efficient querying and storage.

- **Scalability**: 
Built with Spark to handle large-scale weather data processing.

## Project Structure

weatherwise-etl-pipeline/ 
├── config/ │ └── config.yaml # Configuration file for API keys 
├── data/ │ ├── raw/ # Raw weather data stored in Parquet format 
├── processed/ # Processed and transformed data ├── etl/ 
├── extract.py # Handles data extraction from OpenWeatherMap API 
│──transform.py # Transforms raw data into a structured format 
├── load.py # Loads transformed data into storage 
├── tests/ │ └── test_extract.py # Unit tests for the extract module 
├── pipeline.py # Main script to orchestrate the ETL pipeline 
├── requirements.txt # Python dependencies 
└── README.md # Project documentation


## Prerequisites

- Python 3.8 or higher
- Apache Spark
- OpenWeatherMap API key

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/weatherwise-etl-pipeline.git
   cd weatherwise-etl-pipeline

2. Install the required Python dependencies:

pip install -r requirements.txt

3. Configure the config.yaml file:

Add your OpenWeatherMap API key.
  apiKey: "your_api_key_here"

## Usage
1. Run the ETL pipeline:
python pipeline.py

2. The pipeline will:

Fetch weather data for the specified cities.
Transform the data into a structured format.
Save the raw data in data/raw/ and processed data in data/processed/.

Example Output
Raw Data (Parquet Format)
Stored in data/raw/weather_data.parquet:

+---------+-------+------------+--------+-----------+----------+----------+
| city    | country | temperature | humidity | weather   | wind_speed | timestamp |
+---------+-------+------------+--------+-----------+----------+----------+
| New York| US    | 75.2       | 60     | clear sky | 5.1      | 1697049600|
+---------+-------+------------+--------+-----------+----------+----------+

Processed Data
Processed Data
Stored in data/processed/.

## Testing
Run unit tests to ensure the pipeline works as expected:
pytest tests/

## License
This project is licensed under the MIT License. See the LICENSE file for details.

Acknowledgments
- OpenWeatherMap API for providing weather data.
- Apache Spark for enabling scalable data processing.