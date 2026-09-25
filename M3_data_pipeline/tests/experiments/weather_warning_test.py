import os
import json
import requests

from pathlib import Path
from urllib.parse import unquote
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parents[2]

ENV_PATH = (
    PROJECT_DIR
    / "collector"
    / ".env"
)

load_dotenv(
    ENV_PATH,
    override=True
)


API_KEY = unquote(
    os.getenv("WEATHER_WARNING_API_KEY")
)


URL = (
    "https://apis.data.go.kr/"
    "1360000/"
    "WthrWrnInfoService/"
    "getPwnStatus"
)


params = {
    "serviceKey": API_KEY,
    "pageNo": 1,
    "numOfRows": 1000,
    "dataType": "JSON",
}


response = requests.get(
    URL,
    params=params,
    timeout=30
)


print("HTTP:", response.status_code)

print(
    json.dumps(
        response.json(),
        ensure_ascii=False,
        indent=2
    )
)
