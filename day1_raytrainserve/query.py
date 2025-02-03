import requests
import pathlib
import pandas as pd
from configparser import ConfigParser


def _parse_config() -> ConfigParser:
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / "config.ini")
    return config


def main():
    config = _parse_config()
    test_dataset_path = pathlib.Path(__file__).parent / config["FILES"]["test_dataset"]
    test_files = pathlib.Path.glob(test_dataset_path, "*.csv")
    try:
        test_data_path = next(test_files)
        data = pd.read_csv(test_data_path)
        response = requests.post(
            url=config["INFERENCE"]["url"], json=data.to_dict(orient="records")
        )
        if response.status_code == 200:
            print(response.json())
        else:
            raise Exception(f"Error: {response.text}")
    except StopIteration:
        raise Exception("No test dataset found, please run the main script first.")


if __name__ == "__main__":
    main()
