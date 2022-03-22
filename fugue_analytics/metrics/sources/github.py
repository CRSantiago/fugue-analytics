from fugue_analytics.utilities.postgres import connect_to_postgres
import httpx
import pandas as pd
from datetime import datetime

def get_github_star_count(repo="fugue-project/fugue") -> pd.DataFrame:
    """
    Gets the star count of a Github repo
    """
    url = f"https://api.github.com/repos/{repo}"
    res = httpx.get(url).json()
    stars = res['stargazers_count']
    forks = res['forks']

    current_datetime = datetime.now()
    res = pd.DataFrame({"datetime": pd.to_datetime([current_datetime, current_datetime]),
                        "source": ["Fugue Repo Stars", "Fugue Repo Forks"],
                        "value": [stars, forks]})
    return res

if __name__ == "__main__":
    def github_star_count(repo="fugue-project/fugue"):
        res = get_github_star_count(repo)

    github_star_count()