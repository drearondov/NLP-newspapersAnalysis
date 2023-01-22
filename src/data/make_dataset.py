import boto3
import json
import logging
import requests
import sys

from datetime import datetime, timedelta
from dotenv import dotenv_values, find_dotenv
from pathlib import Path
from urllib.parse import urlencode


def get_users_id(newspapers: list, BASE_DIR: str | None, BEARER_TOKEN: str | None) -> dict:
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


def save_to_S3(newspaper: str, newspaper_data: dict, SAVED_DATE: datetime, AWS_KEY_ID:str, AWS_SECRET:str) -> None:
    """Saved recorded data to S3 bucket.

    Args:
        newspaper (str): Name of the newspaper
        newspaper_data (list): Data pulled from the Tweeter API
        saved_date (datetime): Date that the data is being saved
        AWSAccessKeyId (str): AWS access key
        AWSSecretKey (str): AWS secret key
    """

    boto_session = boto3.Session(
            aws_access_key_id=AWS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET
        )
    
    s3 = boto_session.resource("s3")
    
    s3_object = s3.Object(
            "nlp-newspapersanalysis",
            f"raw_data/{SAVED_DATE.isocalendar().year}w{SAVED_DATE.isocalendar().week}_data_{newspaper}.json"
        )


    s3_object.put( Body=(bytes(json.dumps(newspaper_data).encode('UTF-8'))), ContentType='application/json')


def get_users_tweets(
        newspapers_id: dict[str,str],
        start_time: str,
        end_time: str,
        BASE_DIR: str | None,
        BEARER_TOKEN: str | None,
        AWS_KEY_ID:str,
        AWS_SECRET:str
    ) -> None:
    """Gets tweets for newspapers specified between start and end times.

    Args:
        newspapers_id (dict): Dictionary containing tweeter handle and user_id for target newspapers.
        start_time (str): Start date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
        end_time (str): End date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
    """
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

    DATA_DIR = f"{BASE_DIR}/data/raw"
    SAVED_DATE = datetime.fromisoformat(end_time)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        stream = sys.stdout, 
        format = log_format, 
        level = logging.INFO
    )

    logger = logging.getLogger()

    for newspaper, newspaper_id in newspapers_id.items():
        query = {
        "max_results": 100,
        "tweet.fields": "id,text,created_at,public_metrics,possibly_sensitive,referenced_tweets",
        "start_time": f"{start_time}Z",
        "end_time": f"{end_time}Z",
        }
        payload = urlencode(query, safe=",:")
        
        tweets_url = f"https://api.twitter.com/2/users/{newspaper_id}/tweets"
        response = requests.get(tweets_url, headers=headers, params=payload)

        logger.info(f"{newspaper}: status code {response.status_code}")

        try:
            response_data = response.json()["data"] # List of tweets
            response_meta = response.json()["meta"]
        except KeyError:
            logger.info(f"{newspaper}: No data found!")
            continue

        try:
            next_token = response_meta["next_token"]
            query["pagination_token"] = next_token
            payload = urlencode(query, safe=",:")
        except KeyError:
            logger.info(f"{newspaper}: No MORE data found!")

            with open(f"{DATA_DIR}/{SAVED_DATE.isocalendar().year}w{SAVED_DATE.isocalendar().week}_data_{newspaper}.json", "w") as write_file:
                json.dump({"data": response_data}, write_file)
            
            save_to_S3(newspaper, {"data": response_data}, SAVED_DATE, AWS_KEY_ID, AWS_SECRET)
            
            logging.info(f"{newspaper}: Saved! FIRST")
            continue

        while True:
            logger.info(f"{newspaper}: New page")
            new_response = requests.get(tweets_url, headers=headers, params=payload)

            try:
                response_data += new_response.json()["data"]
                response_meta = new_response.json()["meta"]

                next_token = response_meta["next_token"]
                query["pagination_token"] = next_token
                payload = urlencode(query, safe=",:")
            except KeyError:
                logger.info(f"{newspaper}: No MORE data found!")
                
                with open(f"{DATA_DIR}/{SAVED_DATE.isocalendar().year}w{SAVED_DATE.isocalendar().week}_data_{newspaper}.json", "w") as write_file:
                    json.dump({"data": response_data}, write_file)
                
                save_to_S3(newspaper, {"data": response_data}, SAVED_DATE, AWS_KEY_ID, AWS_SECRET)
                
                logging.info(f"{newspaper}: Saved!")
                break


def get_data() -> None:
    """Gets data from twitter API. Takes today's date and gets all data from the past week
    """

    dotenv_path = find_dotenv()
    env_variables = dotenv_values(dotenv_path)

    AWSAccessKeyId = env_variables["AWSAccessKeyId"]
    AWSSecretKey = env_variables["AWSSecretKey"]

    BASE_DIR = env_variables["BASE_DIR"]
    BEARER_TOKEN = env_variables["BEARER_TOKEN"]

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

    if Path(f"{BASE_DIR}/data/raw/newspapers_id.json").exists():
        with open(f'{BASE_DIR}/data/raw/newspapers_id.json', 'r') as read_file:
            newspapers_id = json.load(read_file)
    else:
        newspapers_id = get_users_id(newspapers, BASE_DIR, BEARER_TOKEN)

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)

    get_users_tweets(newspapers_id, last_monday.isoformat(), this_monday.isoformat(), BASE_DIR, BEARER_TOKEN, AWSAccessKeyId, AWSSecretKey)

if __name__ == "__main__":
    get_data()
