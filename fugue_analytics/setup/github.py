from fugue_analytics.metrics.sinks.postgres import insert_df_to_table
import pandas as pd

# @task(retries = 3)
def initialize_github_stats():
    """
    Inserts initial Github stats to the database if starting cold
    """
    df = pd.read_csv("../data/github-star-history.csv", header=None)
    df.columns = ["source", "datetime", "value"]
    df = df[["datetime", "source", "value"]]
    df["source"] = "Fugue Repo Stars"
    df["datetime"] = df["datetime"].str.split(" ").str[1:4].str.join(" ")
    df["datetime"] = pd.to_datetime(df["datetime"], format="%b %d %Y")

    insert_df_to_table(df, "metrics_over_time")
    print("Successfully Inserted Github Stars!")
    return

if __name__ == "__main__":
    initialize_github_stats()