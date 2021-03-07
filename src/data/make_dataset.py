import json
import os
import requests

from dotenv import load_dotenv, find_dotenv
from urllib.parse import urlencode


dotenv_path = find_dotenv()
load_dotenv(dotenv_path)


BASE_DIR = os.environ.get("BASE_DIR")
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")


def get_users_id(newspapers: list) -> dict:
    """Gets user id from target newspapers and saves it to a json file.

    Args:
        newspapers (list): Twitter handle for target newspapers
    """
    newspapers_id = {}
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

    for newspaper in newspapers:
        username_url = f"https://api.twitter.com/2/users/by/username/{newspaper}"
        response = requests.get(username_url, headers=headers)

        newspapers_id[newspaper] = response.json()["data"]["id"]

    with open(f'{BASE_DIR}/data/raw/newspapers_id.json', 'w') as fp:
        json.dump(newspapers_id, fp)

    return newspapers_id


def get_users_tweets(newspapers_id: dict[str,str], start_time: str, end_time: str) -> None:
    """Gets tweets for newspapers specified between start and end times.

    Args:
        newspapers_id (dict): Dictionary containing tweeter handle and user_id for target newspapers.
        start_time (str): Start date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
        end_time (str): End date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
    """
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    DATA_DIR = f"{BASE_DIR}/data/raw/{start_time[0:10]}"

    os.mkdir(DATA_DIR)

    for newspaper, newspaper_id in newspapers_id.items():
        query = {
        "max_results": 100,
        "tweet.fields": "id,text,created_at,public_metrics,possibly_sensitive,referenced_tweets",
        "start_time": start_time,
        "end_time": end_time,
        }
        payload = urlencode(query, safe=",:")

        tweets_url = f"https://api.twitter.com/2/users/{str(newspaper_id)}/tweets"
        response = requests.get(tweets_url, headers=headers, params=payload)
        print(f"{newspaper}: status code {response.status_code}")

        try:
            response_data = response.json()["data"] # List of tweets
            response_meta = response.json()["meta"]

            next_token = response_meta["next_token"]
            query["pagination_token"] = next_token
            payload = urlencode(query, safe=",:")
        except KeyError:
            continue

        while True:
            new_response = requests.get(tweets_url, headers=headers, params=payload)

            response_data += new_response.json()["data"]

            response_meta = new_response.json()["meta"]

            try:
                next_token = response_meta["next_token"]
                query["pagination_token"] = next_token
                payload = urlencode(query, safe=",:")
            except KeyError:
                break

        with open(f"{DATA_DIR}/data_{newspaper}.json", "w") as write_file:
            json.dump({"data": response_data}, write_file)


def main(start_time: str, end_time: str) -> None:
    """Scrapes and process dataset for production.
    """

    newspapers = [
        "elcomercio_peru",
        "larepublica_pe",
        "peru21noticias",
        "tromepe",
        "Gestionpe",
        "diariocorreo",
        "ExpresoPeru",
        "diarioojo",
        "DiarioElPeruano",
        "larazon_pe"
    ]

    newspapers_id = get_users_id(newspapers)

    get_users_tweets(newspapers_id, start_time, end_time)
