from pymongo import MongoClient
import pprint

import requests

from bs4 import BeautifulSoup

import copy

import pandas as pd

client = MongoClient('localhost', 27017)
db = client.sports_betting_capstone
pages = db.pages
nfl_lines = db.nfl_lines

url_nfl2018_1 = 'https://www.vegasinsider.com/nfl/matchups/matchups.cfm/week/'
url_nfl2018_2 = '/season/2018'

for i in range(1,23):
    r = requests.get(url_nfl2018_1 + str(i) + url_nfl2018_2)
    pages.insert_one({'html': r.content})

    soup = BeautifulSoup(r.content, "html")

    divs = soup.findAll("div", {"class": "SLTables1"})

    tables = [] # list accumulator for tables

    for div in divs[2:]: #skipping first 2 divs since these are not specific to games
        table = div.find("table")
        tables.append(table)

    all_rows = []

    empty_row = {
        "Team": None, "Win-Loss": None, "Streak": None, "Open": None, "Closing": None, "Consensus_Spread": None,
        "Consensus_Money": None,"Consensus_OU": None, "Season": 2018, "Week": i
    }

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[4:6]:
            new_row = copy.copy(empty_row)
            # A list of all the entries in the row.
            columns = row.find_all("td")
            new_row['Team'] = columns[0].text.strip()
            new_row['Win-Loss'] = columns[1].text.strip()
            new_row['Streak'] = columns[2].text.strip()
            new_row['Open'] = columns[3].text.strip()
            new_row['Closing'] = columns[4].text.strip()
            new_row['Consensus_Spread'] = columns[5].text.strip()
            new_row['Consensus_Money'] = columns[6].text.strip()
            new_row['Consensus_OU'] = columns[7].text.strip()
            all_rows.append(new_row)

    for row in all_rows:
        nfl_lines.insert_one(row)