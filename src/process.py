"""This a sample of code for accessing to specific files in a datalake
    Contains [datalake] access and files fetching tools
"""

from concurrent.futures import ProcessPoolExecutor
import re  # for regex
import os
import argparse
import json
from datetime import datetime
from dask import bag as bg
from sentiment_analysis import SentimentModel
from clean_tools import cleaning
from connect import insert


def get_args():
    """
        Get args for cmd
    Args:
        None
    Returns:
        args([list]) : list of args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_time",
                        help="format: 2022-01-01", type=str)
    parser.add_argument("-e", "--end_time",
                        help="format: 2022-01-01", type=str)
    args = parser.parse_args()
    return args


def get_files_path(start_date: str, end_date: str):
    """
        Get all files path in a range of date

    Args:
        input_folder ([str]): path du datalake
        start_date ([str]): date de debut
        end_date ([str]): date de fin

    Returns:
        list([str]) : list of paths
    """
    with open("conf/credential.json")as f:
        credentials = json.load(f)
    input_folder = credentials["DATALAKE"]

    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    start_path = r'({}year={}/month={}/day={}/\w+\.json)'.\
                 format(input_folder, start_date.year,
                        start_date.month, start_date.day)

    end_path = r'({}year={}/month={}/day={}/\w+\.json)'.\
               format(input_folder, end_date.year,
                      end_date.month, end_date.day)
    pattern = r'({}year=\d+/month=\d+/day=\d+/\w+\.json)'.format(input_folder)
    # get all files path in our datalake
    repertories = []
    for dirname, dirnames, filenames in os.walk(input_folder):
        for filename in filenames:
            repertories.append(os.path.join(dirname, filename))
    # get only paths of article files
    paths = [p for p in repertories if re.search(pattern, p)]
    # get only path of article in the date range
    start_index = [i for i, word in enumerate(paths)
                   if re.search(start_path, word)]
    end_index = [i for i, word in enumerate(paths)
                 if re.search(end_path, word)]
    final_paths = paths[end_index[0]:start_index[-1]+1]
    return final_paths


def extract(paths: list):
    """
        Read file contents. Save it in dask.bag
    Args :
        paths([list]): list of paths
    Returns:
        dask.bag : bag of articles
    """
    return bg.read_text(paths).map(json.loads)


def transform_articles(article: dict):
    """
        select and modify columns
    Args:
        article([dict]) : an instance of article
    Returns:
        dict
    """
    return {
        'ID': article['_id'],
        'abstract': article['abstract'],
        'url': article['web_url'],
        'publish_date': article['pub_date'],
        'source': article['source'],
        'sentiment': SentimentModel.get_sentiment(article['abstract']),
        'clean_abstract': " ".join(cleaning(article['abstract']))
    }


def main():
    paths = get_files_path(
            start_date=get_args().start_time,
            end_date=get_args().end_time)
    articles = extract(paths)
    with ProcessPoolExecutor() as executor:
        articles_good = list(executor.map(transform_articles, articles))
    insert(articles_good)


if __name__ == '__main__':
    main()


