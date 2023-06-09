import pandas as pd
import os
from prefect import flow, task

def get_domain(url):
    return url.split("://")[1].split("/")[0]

@task
def get_domain_of_url():
    data = pd.read_csv('original.csv')
    data['domain'] = data['url'].apply(get_domain)
    data.to_csv('result2.csv')
    return data


@flow(log_prints=True, name="prefect_getdomain")
def run():
    get_domain_of_url()


if __name__ == "__main__":
    run()
