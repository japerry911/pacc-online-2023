# with a deployment, if not writing out to absolute file path
# or saving to Cloud or Docker volume,
# output will be written to temporary directory and deleted
# so we'll just persist the result to storage

import httpx
from prefect import flow, task


@task
def fetch_cat_fact():
    return httpx.get("https://catfact.ninja/fact?max_length=140").json()["fact"]


@task(persist_result=True)
def formatting(fact: str):
    return fact.title()


@flow
def pipe():
    fact = fetch_cat_fact()
    formatted_fact = formatting(fact)

    print(formatted_fact)


if __name__ == "__main__":
    pipe()


# Use a flow from the 103 lab
# - Make a deployment via interactive CLI commands
# - Start a worker
# - Run the deployment
# - See the flow run logs in the UI
# - Stretch: Create a deployment that has default parameters & override the default parameters at runtime
