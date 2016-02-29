import pandas as pd
import sqlite3 as lite
import numpy as np
import pdb

conn = lite.connect('./data/database.sqlite')

regular_season_query = "SELECT * FROM RegularSeasonDetailedResults;"
teams_query = "SELECT * FROM Teams;"

regular_season_df = pd.read_sql(regular_season_query, conn)
teams_df = pd.read_sql(teams_query, conn)
conn.rollback()

def calc_team_season_totals(dataframe):
  winner_categories = [
        'Wscore', 'Wfgm', 'Wfga', 'Wfgm3', 'Wfga3','Wftm', 'Wfta', 'Wor', 'Wdr','Wast', 'Wto', 'Wstl', 'Wblk', 'Wpf', 
      ]      
  loser_categories = [
        'Lscore', 'Lfgm', 'Lfga', 'Lfgm3', 'Lfga3','Lftm', 'Lfta', 'Lor', 'Ldr', 'Last', 'Lto', 'Lstl', 'Lblk', 'Lpf'
      ]    
  
  feature_dict = {}

  for i, row in dataframe.iterrows():
    winner = row['Wteam']
    loser = row['Lteam']

    for category in winner_categories:
      category_key = category.lstrip(category[0])
      
      if winner in feature_dict.keys():
        if category_key in feature_dict[winner].keys():
          feature_dict[winner][category_key].append(row[category])  
        else:
          feature_dict[winner][category_key] = [row[category]]
      else:
        feature_dict[winner] = {}
        feature_dict[winner][category_key] = [row[category]]

    for category in loser_categories:
      category_key = category.lstrip(category[0])
      
      if loser in feature_dict.keys():
        if category_key in feature_dict[loser].keys():
          feature_dict[loser][category_key].append(row[category]) 
        else:
          feature_dict[loser][category_key] = [row[category]]
      else:
        feature_dict[loser] = {}
        feature_dict[loser][category_key] = [row[category]]        

  return feature_dict


def calc_per_game_averages(feature_dict):
  for team, category_list in feature_dict.items():
    for category, game_totals in category_list.items():
      category['average'] =  np.array(game_totals).mean()
  return feature_dict

def check_keys(team, category, feature_dict):
    try:
        return calc_per_game_averages(feature_dict)[team][category]['average']
    except:
        return None

def yearly_team_features(teams_df, games_df):
    years = [2012, 2013, 2014, 2015]
    for year in years:
        games_year = games_df[games_df['Season'] == year]
        games_year_stats = calc_team_season_totals(games_year)
        categories = list(list(games_year_stats.values())[0].keys())
        for category in categories:
          teams_df[category + '_{}'.format(str(year))] = teams_df['Team_Id'].apply(lambda x: check_keys(x, category, games_year_stats))
    
    return teams_df

teams_with_yearly_per_game_averages = yearly_team_features(teams_df, regular_season_df)    

pdb.set_trace()
teams_with_yearly_per_game_averages.head().to_sql('team_data', conn, if_exists='replace')

# Data Structures: 

# {"Duke": {"3-pointers-for": [8, 12, 4, 13, 9],
#             "3-pointers-allowed": []

#     }
# }
# Output table: 

# 1 | Duke | +150 | 4.5 | 3.5 | 
