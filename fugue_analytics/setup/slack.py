from fugue_analytics.metrics.sinks.postgres import insert_df_to_table
import pandas as pd

# @task(retries = 3)
def initialize_slack_stats():
    """
    Inserts initial Slack channel joins
    """
    df = pd.DataFrame({"datetime": pd.to_datetime(["2020-02-24", "2022-03-22"]),
                       "source": ["Slack Channel Joins", "Slack Channel Joins"],
                       "value": [105, 116]})
    
    insert_df_to_table(df, "metrics_over_time")
    print("Successfully Inserted Slack Channel Joins!")
    return

if __name__ == "__main__":
    initialize_slack_stats()