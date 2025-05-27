import yaml
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
            config = yaml.safe_load(file)

            # Inject API key from environment variables
            config['apiKey'] = os.getenv('WEATHERWISE_API_KEY')
            return config
    except FileNotFoundError:
        print(f"Configuration file {config_file} not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None