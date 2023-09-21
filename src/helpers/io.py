import boto3
import json


def save_to_S3(
        target_folder: str,
        data: dict[str, object],
        AWS_KEY_ID: str,
        AWS_SECRET: str
) -> None:
    """Saves data to S3 bucked under specified folder.

    Args:
        target_folder (str): Name of the forlder where the data is going to be saved
        data (dict[str, object]): Dictionary with the following keys: saved_date['year', 'week'], data and file_name
        AWS_KEY_ID (str): _description_
        AWS_SECRET (str): _description_
    """
    boto_session = boto3.Session(
        aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET
    )

    s3 = boto_session.resource("s3")

    s3_object = s3.Object(
        "nlp-newspapersanalysis",
        f"{target_folder}/{data['saved_date']['year']}w{data['saved_date']['week']}_{data['file_name']}.json",
    )

    s3_object.put(
        Body=(bytes(json.dumps(data["data"]).encode("UTF-8"))),
        ContentType="application/json",
    )
