# Import all packages we need
from urllib.parse import quote_from_bytes
from webbrowser import get
from numpy import block
import requests   # send HTTP requests
import json       # work with json file
from datetime import datetime
import os         # interact with system
import argparse


def get_args():
    """
        Get args for cmd
    Args:
        None
    Returns:
        args([list]) : list of args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--keyword", help="Enter your keyword",
                        type=str, required=True)
    args = parser.parse_args()
    return args


def get_credentials():
    """
        Get credential into the credential file

    Args:
        None

    Returns:
        list: credentials list
    """
    with open("conf/credential.json")as f:
        credentials = json.load(f)
    API_KEY = credentials["API_KEY"]
    BASE_URL = credentials["BASE_URL"]
    API_PATH = credentials["API_PATH"]
    DATALAKE = credentials["DATALAKE"]

    return [API_KEY, BASE_URL, API_PATH, DATALAKE]


def get_articles(keyword: str):
    """
        Get 100 last articles about the keyword
        save into the datalake
    Args:
        keyword([string]): Keyword for the search
    Returns:
        None
    """

    # constantes
    API_KEY, BASE_URL, API_PATH, DATALAKE = get_credentials()

    page = 1
    URL = BASE_URL + API_PATH + "q={}&page={}&api-key={}".\
        format(keyword, page, API_KEY)
    # getting articles
    print("Loading...")
    articles = []
    for page in range(10):
        response = requests.get(URL)
        if response.ok:
            articles.extend(response.json()["response"]["docs"])
    print("Getting OK")
    # save into datalake
    for article in articles:
        id = article["_id"].split('/')[-1]. replace("-", "")
        pub_date = datetime.\
            strptime(article["pub_date"], "%Y-%m-%dT%H:%M:%S+0000")
        folder_path = os.path.join(DATALAKE+"year={}/month={}/day={}".format(pub_date.year, pub_date.month, pub_date.day))
        os.makedirs(folder_path, exist_ok=True)
        file = folder_path + "/" + id + ".json"
        with open(file, 'w') as f:
            json.dump(article, f)
    print("Operation OK")


def main():
    get_articles(get_args().keyword)


if __name__ == '__main__':
    main()
