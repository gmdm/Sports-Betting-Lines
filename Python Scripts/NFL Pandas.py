import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.sports_betting_capstone
nfl_lines = db.nfl_lines
nfl_results = db.nfl_resultsv2

nfl_lines_row = nfl_lines.find()
nfl_lines_pddf = pd.DataFrame(list(nfl_lines_row))
nfl_lines_pddf

nfl_lines_pddf['Open'].replace(to_replace = 'PK', value = 0, inplace = True)
nfl_lines_pddf['Closing'].replace(to_replace = 'PK', value = 0, inplace = True)

nfl_lines_pddf['Open'] = pd.to_numeric(nfl_lines_pddf['Open'])
nfl_lines_pddf['Closing'] = pd.to_numeric(nfl_lines_pddf['Closing'])

for idx, row in nfl_lines_pddf.iterrows():
    if idx == 0 or idx % 2 == 0:
        nfl_lines_pddf.loc[idx, 'Include'] = True
        nfl_lines_pddf.loc[idx, 'Team 1'] = nfl_lines_pddf.loc[idx, 'Team']
        nfl_lines_pddf.loc[idx, 'Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Team']
        if nfl_lines_pddf.loc[idx, 'Open'] > nfl_lines_pddf.loc[idx + 1, 'Open']:
            nfl_lines_pddf.loc[idx, 'Spread Open'] = nfl_lines_pddf.loc[idx + 1, 'Open']
            nfl_lines_pddf.loc[idx, 'O/U Open'] = nfl_lines_pddf.loc[idx, 'Open']
            if nfl_lines_pddf.loc[idx + 1, 'Open'] == 0:
                nfl_lines_pddf.loc[idx,'Open Favorite'] = 'No Favorite'
            else:
                nfl_lines_pddf.loc[idx,'Open Favorite'] = 'Team 2'
        else:
            nfl_lines_pddf.loc[idx, 'Spread Open'] = nfl_lines_pddf.loc[idx, 'Open']
            nfl_lines_pddf.loc[idx, 'O/U Open'] = nfl_lines_pddf.loc[idx + 1, 'Open']
            if nfl_lines_pddf.loc[idx, 'Open'] == 0:
                nfl_lines_pddf.loc[idx,'Open Favorite'] = 'No Favorite'
            else:
                nfl_lines_pddf.loc[idx,'Open Favorite'] = 'Team 1'
        if nfl_lines_pddf.loc[idx, 'Closing'] > nfl_lines_pddf.loc[idx + 1, 'Closing']:
            nfl_lines_pddf.loc[idx, 'Spread Close'] = nfl_lines_pddf.loc[idx + 1, 'Closing']
            nfl_lines_pddf.loc[idx, 'O/U Close'] = nfl_lines_pddf.loc[idx, 'Closing']
            if nfl_lines_pddf.loc[idx + 1, 'Closing'] == 0:
                nfl_lines_pddf.loc[idx,'Close Favorite'] = 'No Favorite'
            else:
                nfl_lines_pddf.loc[idx,'Close Favorite'] = 'Team 2'
        else:
            nfl_lines_pddf.loc[idx, 'Spread Close'] = nfl_lines_pddf.loc[idx, 'Closing']
            nfl_lines_pddf.loc[idx, 'O/U Close'] = nfl_lines_pddf.loc[idx + 1, 'Closing']
            if nfl_lines_pddf.loc[idx, 'Closing'] == 0:
                nfl_lines_pddf.loc[idx,'Close Favorite'] = 'No Favorite'
            else:
                nfl_lines_pddf.loc[idx,'Close Favorite'] = 'Team 1'
        nfl_lines_pddf.loc[idx, 'Spread Close'] = min(nfl_lines_pddf.loc[idx, 'Closing'], nfl_lines_pddf.loc[idx + 1, 'Closing'])
        nfl_lines_pddf.loc[idx, 'O/U Close'] = max(nfl_lines_pddf.loc[idx, 'Closing'], nfl_lines_pddf.loc[idx + 1, 'Closing'])
        nfl_lines_pddf.loc[idx, 'Spread Consensus Team 1'] = nfl_lines_pddf.loc[idx, 'Consensus_Spread']
        nfl_lines_pddf.loc[idx, 'Spread Consensus Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_Spread']
        nfl_lines_pddf.loc[idx, 'Over Consensus'] = nfl_lines_pddf.loc[idx, 'Consensus_OU']
        nfl_lines_pddf.loc[idx, 'Under Consensus'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_OU']
        nfl_lines_pddf.loc[idx, 'Moneyline Consensus Team 1'] = nfl_lines_pddf.loc[idx, 'Consensus_Money']
        nfl_lines_pddf.loc[idx, 'Moneyline Consensus Team 2'] = nfl_lines_pddf.loc[idx + 1, 'Consensus_Money']
    else:
        nfl_lines_pddf.loc[idx, 'Include'] = False

clean_nfl_lines_pddf = nfl_lines_pddf.drop(nfl_lines_pddf.columns[range(9)], axis = 1)[nfl_lines_pddf['Include']].reset_index()
clean_nfl_lines_pddf['Team 1'] = clean_nfl_lines_pddf['Team 1'].apply(lambda x: ''.join(filter(str.isalpha, x))).str.replace('Â',"")
clean_nfl_lines_pddf['Team 2'] = clean_nfl_lines_pddf['Team 2'].apply(lambda x: ''.join(filter(str.isalpha, x))).str.replace('Â',"")
clean_nfl_lines_pddf['Season'] = clean_nfl_lines_pddf['Season'].astype(str)
clean_nfl_lines_pddf['Week'] = clean_nfl_lines_pddf['Week'].astype(str)
clean_nfl_lines_pddf['Game ID 1'] = clean_nfl_lines_pddf['Season'] + clean_nfl_lines_pddf['Week'] + clean_nfl_lines_pddf['Team 1'] + clean_nfl_lines_pddf['Team 2']
clean_nfl_lines_pddf['Game ID 2'] = clean_nfl_lines_pddf['Season'] + clean_nfl_lines_pddf['Week'] + clean_nfl_lines_pddf['Team 2'] + clean_nfl_lines_pddf['Team 1']

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
clean_nfl_results_pddf['Season'] = clean_nfl_results_pddf['Season'].astype(str)
clean_nfl_results_pddf['Week'] = clean_nfl_results_pddf['Week'].astype(str)
clean_nfl_results_pddf['Game ID 1'] = clean_nfl_results_pddf['Season'] + clean_nfl_results_pddf['Week'] + clean_nfl_results_pddf['Team 1'] + clean_nfl_results_pddf['Team 2']
clean_nfl_results_pddf['Game ID 2'] = clean_nfl_results_pddf['Season'] + clean_nfl_results_pddf['Week'] + clean_nfl_results_pddf['Team 2'] + clean_nfl_results_pddf['Team 1']

combined_nfl_pddf = pd.merge(clean_nfl_lines_pddf, clean_nfl_results_pddf, left_on = ['Game ID 1', 'Game ID 2'], right_on = ['Game ID 1', 'Game ID 2'])

clean_nfl_lines_pddf.to_csv('nfl_lines.csv')
clean_nfl_results_pddf.to_csv('nfl_results.csv')
combined_nfl_pddf.to_csv('combined_output.csv')

# print(clean_nfl_lines_pddf)
# print(clean_nfl_results_pddf)
# print(combined_nfl_pddf)