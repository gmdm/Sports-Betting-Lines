import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.sports_betting_capstone
nfl_lines = db.nfl_lines
nfl_results = db.nfl_results

nfl_lines_row = nfl_lines.find()
nfl_lines_pddf = pd.DataFrame(list(nfl_lines_row))
nfl_lines_pddf

for idx, row in nfl_lines_pddf.iterrows():
    if idx == 0 or idx % 2 == 0:
        nfl_lines_pddf.loc[idx, 'Include'] = True
        nfl_lines_pddf.loc[idx, 'Team 1'] = nfl_lines_pddf.loc[idx, 'Team']
        nfl_lines_pddf.loc[idx, 'Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Team']
        nfl_lines_pddf.loc[idx, 'Spread Open'] = nfl_lines_pddf.loc[idx , 'Open']
        nfl_lines_pddf.loc[idx, 'Spread Close'] = nfl_lines_pddf.loc[idx, 'Closing']
        nfl_lines_pddf.loc[idx, 'Spread Consensus Team 1'] = nfl_lines_pddf.loc[idx, 'Consensus_Spread']
        nfl_lines_pddf.loc[idx, 'Spread Consensus Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_Spread']
        nfl_lines_pddf.loc[idx, 'O/U Open'] = nfl_lines_pddf.loc[idx + 1 , 'Open']
        nfl_lines_pddf.loc[idx, 'O/U Close'] = nfl_lines_pddf.loc[idx + 1, 'Closing']
        nfl_lines_pddf.loc[idx, 'Over Consensus'] = nfl_lines_pddf.loc[idx, 'Consensus_OU']
        nfl_lines_pddf.loc[idx, 'Under Consensus'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_OU']
        nfl_lines_pddf.loc[idx, 'Moneyline Consensus Team 1'] = nfl_lines_pddf.loc[idx, 'Consensus_Money']
        nfl_lines_pddf.loc[idx, 'Moneyline Consensus Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_Money']
    else:
        nfl_lines_pddf.loc[idx, 'Include'] = False

clean_nfl_lines_pddf = nfl_lines_pddf.drop(nfl_lines_pddf.columns[range(9)], axis = 1)[nfl_lines_pddf['Include']].reset_index()
clean_nfl_lines_pddf['Team 1'] = clean_nfl_lines_pddf['Team 1'].apply(lambda x: ''.join(filter(str.isalpha, x)))
clean_nfl_lines_pddf['Team 2'] = clean_nfl_lines_pddf['Team 2'].apply(lambda x: ''.join(filter(str.isalpha, x)))

nfl_results_row = nfl_results.find()
nfl_results_pddf = pd.DataFrame(list(nfl_results_row))
nfl_results_pddf

for idx, row in nfl_results_pddf.iterrows():
    if idx == 0 or idx % 2 == 0:
        nfl_results_pddf.loc[idx, 'Include'] = True
        nfl_results_pddf.loc[idx, 'Team 1'] = nfl_results_pddf.loc[idx, 'Team']
        nfl_results_pddf.loc[idx, 'Team 2'] = nfl_results_pddf.loc[idx + 1, 'Team']
        nfl_results_pddf.loc[idx, 'Team 1 Final'] = int(nfl_results_pddf.loc[idx , 'Final'])
        nfl_results_pddf.loc[idx, 'Team 2 Final'] = int(nfl_results_pddf.loc[idx + 1, 'Final'])
        nfl_results_pddf.loc[idx, 'Total Final'] = int(nfl_results_pddf.loc[idx, 'Final']) + int(nfl_results_pddf.loc[idx + 1, 'Final'])
    else:
        nfl_results_pddf.loc[idx, 'Include'] = False

clean_nfl_results_pddf = nfl_results_pddf.drop(['_id', 'Team', 'Final'], axis = 1)[nfl_results_pddf['Include']].reset_index()
clean_nfl_results_pddf['Team 1'] = clean_nfl_results_pddf['Team 1'].apply(lambda x: ''.join(filter(str.isalpha, x)))
clean_nfl_results_pddf['Team 2'] = clean_nfl_results_pddf['Team 2'].apply(lambda x: ''.join(filter(str.isalpha, x)))

print(clean_nfl_lines_pddf)
print(clean_nfl_results_pddf)