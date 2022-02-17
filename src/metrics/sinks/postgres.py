from prefect import get_run_logger, task
import httpx

@task(retries = 3)
def get_slack_member_count():
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()['stargazers_count']
    print(count)
    return count

if __name__ == "__main__":
    get_github_star_count()