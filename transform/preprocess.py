"""This a sample of code for accessing to specific files in a datalake
    Contains [datalake] access and files fetching tools
"""

from distutils.command.clean import clean
import re  # for regex 
import os
import argparse
import json
from datetime import date, datetime

from dask import bag as bg



from sentiment_analysis import SentimentModel
from clean_tools import  cleaning


def get_args():
    """
        Get args for cmd
    Args:
        None
    Returns:
        args([list]) : list of args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_time", help="format: 2022-01-01", type=str)
    parser.add_argument("-e", "--end_time", help="format: 2022-01-01", type=str)
    args = parser.parse_args()
    return args


def get_files_path(start_date, end_date):
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
    start_path = r'({}year={}/month={}/day={}/\w+\.json)'.format(input_folder, start_date.year, start_date.month, start_date.day)

    end_path = r'({}year={}/month={}/day={}/\w+\.json)'.format(input_folder, end_date.year, end_date.month, end_date.day)
    
    pattern = r'({}year=\d+/month=\d+/day=\d+/\w+\.json)'.format(input_folder)

    repertories = []
    for dirname, dirnames, filenames in os.walk(input_folder):
    # print path to all filenames.
      for filename in filenames:
          repertories.append(os.path.join(dirname, filename))

    paths = [p for p in repertories if re.search(pattern, p)]


    start_index = [i for i, word in enumerate(paths) if  re.search(start_path, word)]
    end_index = [i for i, word in enumerate(paths) if  re.search(end_path, word) ]


    final_paths = paths[end_index[0]:start_index[-1]+1]   
    return final_paths


def read_file(paths):
    """
        Read file contents. Save it in dask.bag
    Args :
        paths([list]): list of paths
    Returns:
        dask.bag : bag of articles
     
    """
    return bg.read_text(paths).map(json.loads)


def transform_articles(article):
    """
        select and modify columns 
    Args: 
        article([dict]) : an instance of article
    Returns:
        dict
    
    """
    return {
        'ID':article['_id'],
        'abstract':article['abstract'],
        'url': article['web_url'],
        'publish_date': article['pub_date'],
        'source' : article['source'], 
        'sentiment': SentimentModel.get_sentiment(article['abstract']),
        'clean_abstract': " ".join(cleaning(article['abstract']))
    }


def main():
    from concurrent.futures import ProcessPoolExecutor

    paths = get_files_path( 
            start_date= get_args().start_time,
            end_date= get_args().end_time )
    articles = read_file(paths)
    with ProcessPoolExecutor() as executor:
        articles_good = list(executor.map(transform_articles, articles))
    return articles_good

if __name__ == '__main__':
    main()


