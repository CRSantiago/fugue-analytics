from fugue_analytics.utilities.postgres import insert_df_to_table
import pandas as pd

# @task(retries = 3)
def initialize_github_stats():
    """
    Inserts initial Github stats to the database if starting cold
    """
    df = pd.read_csv("../data/github-star-history.csv", header=None)
    df.columns = ["source", "date", "value"]
    df = df[["date", "source", "value"]]
    df["source"] = "Fugue Repo Stars"
    df["date"] = df["date"].str.split(" ").str[1:4].str.join(" ")
    df["date"] = pd.to_datetime(df["date"], format="%b %d %Y")

    insert_df_to_table(df, "metrics_over_time")
    print("Successfully Inserted Github Stars!")
    return

if __name__ == "__main__":
    initialize_github_stats()