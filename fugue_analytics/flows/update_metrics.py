from fugue_analytics.metrics.sinks import insert_df_to_table
from fugue_analytics.metrics.sources import (
    get_github_star_count, get_slack_member_count
)
from fugue_analytics.setup import (
    drop_table, 
    create_metrics_table, 
    initialize_github_stats, 
    initialize_slack_stats
)
from prefect import flow, task

@flow
def update_metrics(initialize:bool = False):
    if initialize:
        print(f"Initialize was set to True")
        drop_table()
        create_metrics_table()
        initialize_github_stats()
        initialize_slack_stats()

    slack_df = get_slack_member_count()
    github_df = get_github_star_count()
    insert_df_to_table(slack_df, "metrics_over_time")
    insert_df_to_table(github_df, "metrics_over_time")

if __name__ == "__main__":
    update_metrics()