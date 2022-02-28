from prefect import get_run_logger, task
import os
import requests as re
from fugue_analytics.utilities.postgres import execute_query
import pandas as pd
import json
from datetime import date, datetime

@task(retries = 3)
def get_slack_member_count() -> pd.DataFrame:
    """
    Slack doesn't seem to have an API to get workspace members so we 
    need to use Orbit to get the join events and then add it to the
    previous total retrieves from the database

    This is the sole reason the metrics table needs datetime resolution
    """
    token = os.environ["ORBIT_TOKEN"]
    current_datetime = datetime.now()
    current_date = date.today().strftime("%Y-%m-%d")
    url = f"https://app.orbit.love/api/v1/fugue/activities?activity_type=slack%3Achannel%3Ajoined&start_date={current_date}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = re.get(url, headers=headers)
    df = pd.json_normalize(json.loads(response.text)["data"])
    print(df.head())

    # Pull latest record from database
    query = """SELECT source, MAX(datetime), MAX(value)
               FROM metrics_over_time
               WHERE source = 'Slack Channel Joins'
               GROUP BY source
            """
    latest = execute_query(query)
    print(latest)
    latest_time = latest[0][1]
    latest_val = latest[0][2]

    # Filtering records already counted
    # TO-DO: account for datetime
    if df.shape[0] > 0:
        # Orbit time comes in UTC
        filtered_df = df.loc[pd.to_datetime(df["attributes.created_at"]).dt.tz_convert('US/Eastern') >= current_datetime]

        if filtered_df.shape[0] > 1:
            res = pd.DataFrame({"datetime": pd.to_datetime([current_datetime]),
                            "source": ["Slack Channel Joins"],
                            "value": [latest_val + filtered_df.shape[0]]})
            return res
    return pd.DataFrame()

if __name__ == "__main__":
    @flow()
    def github_slack_count():
        res = get_slack_member_count()

    github_slack_count()