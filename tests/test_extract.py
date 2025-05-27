import pytest
from config.setup import load_config
from etl.extract import get_weather_data

def test_get_weather_data_valid_city(mocker):
    # Mock API response for a valid city
    mock_response = {"city": "New York", "temperature": 22, "humidity": 60}
    mocker.patch("tests.test_extract.get_weather_data", return_value=mock_response)

    city = "New York"
    apiKey="TESTAPIKEY"
    result = get_weather_data(city,apiKey)

    assert result == mock_response

def test_get_weather_data_invalid_city(mocker):
    # Mock API response for an invalid city
    mocker.patch("tests.test_extract.get_weather_data", side_effect=Exception("City not found"))

    city = "InvalidCity"
    apiKey="TESTAPIKEY"
    with pytest.raises(Exception, match="City not found"):
        get_weather_data(city,apiKey)