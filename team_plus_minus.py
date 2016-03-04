import pandas as pd
import sqlite3 as lite
import pdb 

conn = lite.connect('./data/database.sqlite')

regular_season_query = "SELECT * FROM RegularSeasonCompactResults;"
teams_query = "SELECT * FROM Teams;"

regular_season_df = pd.read_sql(regular_season_query, conn)
regular_season_df['diff'] = regular_season_df['Wscore'] - regular_season_df['Lscore']
teams_df = pd.read_sql(teams_query, conn)
conn.rollback()

def calc_diffs(dataframe):
    diff_dict = {}
    for i, row in dataframe.iterrows():
        winner = row['Wteam']
        loser = row['Lteam']
        diff = row['diff']
        if winner in diff_dict.keys():
            diff_dict[winner] += diff
        else:
            diff_dict[winner] = diff
        if loser in diff_dict.keys():
            diff_dict[loser] -= diff
        else:
            diff_dict[loser] = (0 - diff)
    return diff_dict

def yearly_team_plus_minus(teams_df, games_df):
    years = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015]
    for year in years:
        games_year = games_df[games_df['Season'] == year]
        games_year_diffs = calc_diffs(games_year)
        teams_df['SeasonPlusMinus_{}'.format(str(year))] = teams_df['Team_Id'].apply(lambda x: check_keys(x, games_year_diffs))
    return teams_df

def check_keys(key, dict):
    try:
        return dict[key]['average']
    except:
        return None

teams_with_yearly_plus_minus = yearly_team_plus_minus(teams_df, regular_season_df)

teams_with_yearly_plus_minus.to_sql('team_data', conn, if_exists='replace')