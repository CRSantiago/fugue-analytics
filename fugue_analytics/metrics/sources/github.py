from prefect import get_run_logger, task, flow
import httpx

# @task(retries = 3)
def get_github_star_count(repo="fugue-project/fugue"):
    url = f"https://api.github.com/repos/{repo}"
    res = httpx.get(url).json()
    stars = res['stargazers_count']
    forks = res['forks']
    return stars, forks

if __name__ == "__main__":
    # get_github_star_count()
    stars, forks = get_github_star_count()
    print(f"There are this many stars: {stars}")
    print(f"There are this many forks: {forks}")