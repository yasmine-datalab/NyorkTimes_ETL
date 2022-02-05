from datetime import date
from urllib import request
import mysql.connector
import json


def create_db():
    """
        Create database
    Args:
        None
    Returns:
        None
    """
    with open("conf/credential.json")as f:
        credentials = json.load(f)
    HOST = credentials["HOST"]
    USER = credentials["USER"]
    PASSWD = credentials["PASSWD"]
    db_connection = mysql.connector.connect(
                    host=HOST,
                    user=USER,
                    passwd=PASSWD,
                    auth_plugin='mysql_native_password'
                    )
    # creating database_cursor to perform SQL operation
    db_cursor = db_connection.cursor()
    request = """
            create database if not exists DATAMART;
    """
    # executing cursor with execute method and pass SQL query
    db_cursor.execute(request)


def create_table():
    """
        Create table
    Args:
        None
    Returns:
        None
    """
    with open("conf/credential.json")as f:
        credentials = json.load(f)
    HOST = credentials["HOST"]
    USER = credentials["USER"]
    PASSWD = credentials["PASSWD"]
    db_connection = mysql.connector.connect(
                    host=HOST,
                    user=USER,
                    passwd=PASSWD,
                    database="DATAMART",
                    auth_plugin='mysql_native_password'
                    )
    request = """
            create table if not exists Articles(
            _id INT AUTO_INCREMENT PRIMARY KEY,
            ID VARCHAR(255), lien TEXT, publish_date VARCHAR(100),
            source VARCHAR(255), sentiment VARCHAR(15),
            clean_abstract TEXT
            );
    """
    db_cursor = db_connection.cursor()
    db_cursor.execute(request)


def insert(articles: list):
    """
      insert into table
    Args:
        articles([list]) : list of dict
    Returns:
        None
    """
    create_db()
    create_table()
    with open("conf/credential.json")as f:
        credentials = json.load(f)
    HOST = credentials["HOST"]
    USER = credentials["USER"]
    PASSWD = credentials["PASSWD"]
    db_connection = mysql.connector.connect(
                    host=HOST,
                    user=USER,
                    passwd=PASSWD,
                    database="DATAMART",
                    auth_plugin='mysql_native_password'
                    )
    db_cursor = db_connection.cursor()
    for article in articles:
        request = """INSERT INTO Articles (ID, lien, publish_date , 
                    source, sentiment, clean_abstract)
                    VALUES (%s, %s, %s,%s, %s, %s)"""
        val = (str(article['ID']),
                str(article['url']), str(article['publish_date']),
                str(article['source']), str(article['sentiment']),
                str(article['clean_abstract']))
        db_cursor.execute(request, val)
    db_connection.commit()
    print("INSERTION EFFECTUEE AVEC SUCCES")

