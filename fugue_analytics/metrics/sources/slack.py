from prefect import get_run_logger, task
import os
import requests as re

# @task(retries = 3)
def get_slack_member_count():
    """
    Slack doesn't seem to have an API to get workspace members so we 
    need to use Orbit to get the join events and then add it to the
    previous total retrieves from the database
    """
    token = os.environ["ORBIT_TOKEN"]
    url = "https://app.orbit.love/api/v1/fugue/activities?activity_type=slack%3Achannel%3Ajoined&start_date=2022-02-25"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = re.get(url, headers=headers)
    print(response.text)
    return 1

if __name__ == "__main__":
    get_slack_member_count()