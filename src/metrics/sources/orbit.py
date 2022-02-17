from prefect import get_run_logger, task
import httpx
import os

@task(retries = 3)
def get_slack_member_count():
    os.environ["ORBIT_TOKEN"]
    return 1

if __name__ == "__main__":
    get_slack_member_count()