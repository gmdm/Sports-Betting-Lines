from pymongo import MongoClient
import pprint

import requests

from bs4 import BeautifulSoup

import copy

import pandas as pd

client = MongoClient('localhost', 27017)
db = client.sports_betting_capstone
pages = db.pages
nfl_results = db.nfl_results

url_nfl_1 = 'https://www.footballdb.com/scores/index.html?lg=NFL&yr='
url_nfl_2 = '&type=reg&wk='

headers = headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

for year in range(2020,2021):
    for week in range(1,8):
        r = requests.get(url_nfl_1 + str(year) + url_nfl_2 + str(week), headers = headers)
        pages.insert_one({'html': r.content})

        soup = BeautifulSoup(r.content, "html")

        divs = soup.findAll("div", {"class": "lngame"})

        tables = [] # list accumulator for tables

        for div in divs:
            table = div.find("table")
            tables.append(table)

        all_rows = []

        empty_row = {
            "Team": None, "Final": None, "Season": year, "Week": week
        }

        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:
                new_row = copy.copy(empty_row)
                # A list of all the entries in the row.
                columns = row.find_all("td")
                new_row['Team'] = columns[0].text.strip()
                new_row['Final'] = columns[-1].text.strip()
                all_rows.append(new_row)

        for row in all_rows:
            nfl_results.insert_one(row)