import requests

from dotenv import dotenv_values, find_dotenv
from urllib.parse import urlencode


dotenv_path = find_dotenv()
env_variables = dotenv_values(dotenv_path)

BASE_DIR = env_variables["BASE_DIR"]
BEARER_TOKEN = env_variables["BEARER_TOKEN"]


headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

query = {
    "ids": "1547732445729918977",
    "tweet.fields": "id,conversation_id",
}
payload = urlencode(query, safe=",:")

tweets_url = "https://api.twitter.com/2/tweets"
response = requests.get(tweets_url, headers=headers, params=payload)
