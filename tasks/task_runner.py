from invoke import task

from ..src.data.make_dataset import get_data


@task(help={
    "start_time": "Start date for tweets with '%Y-%m-%dT%H:%M:%sZ' format",
    "end_time": "End date for tweets with '%Y-%m-%dT%H:%M:%sZ' format",
})
def get_tweets(ctx, start_time: str, end_time:str) -> None:
    """Gets data from Twitter and deposits on the corresponding file

    Args:
        start_time (str): Start date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
        end_time (str): End date for tweets with '%Y-%m-%dT%H:%M:%sZ' format
    """
    get_data(start_time, end_time)
