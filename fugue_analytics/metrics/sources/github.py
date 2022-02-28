from prefect import get_run_logger, task, flow
import httpx

@task(retries = 3)
def get_github_star_count(repo="fugue-project/fugue"):
    url = f"https://api.github.com/repos/{repo}"
    res = httpx.get(url).json()
    stars = res['stargazers_count']
    forks = res['forks']

    logger = get_run_logger()
    logger.info(f"There are this many stars: {stars}")
    logger.info(f"There are this many forks: {forks}")

    return stars, forks

if __name__ == "__main__":
    @flow()
    def github_star_count(repo="fugue-project/fugue"):
        stars, forks = get_github_star_count(repo).wait().result()

    github_star_count()