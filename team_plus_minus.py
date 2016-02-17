import pandas as pd
import sqlite3 as lite

conn = lite.connect('./data/database.sqlite')
regular_season_query = "SELECT * FROM RegularSeasonCompactResults;"
teams_query = "SELECT * FROM Teams;"
regular_season_df = pd.read_sql(regular_season_query, conn)
regular_season_df = regular_season_df[regular_season_df['Year']]
teams_df = pd.read_sql(teams_query, conn)
conn.rollback()

def calc_diffs(dataframe):
    diff_dict = {}
    for i, row in dataframe.iterrows():
        winner = row.Wteam
        loser = row.Lteam
        diff = row.diff
        if winner in diff_dict.keys():
            diff_dict[winner] += diff
        else:
            diff_dict[winner] = diff
        if loser in diff_dict.keys():
            diff_dict[loser] -= diff
        else:
            diff_dict[loser = (-1 * diff)]
    return diff_dict

def yearly_team_plus_minus(teams_df, games_df):
    years = [2012, 2013, 2014, 2015]
    for year in years:
        games_year = games_df[games_df['Year'] == year]
        teams_df['SeasonPlusMinus_{}'.format(str(year)] = teams['TeamId'].applymap(lambda x: calc_diffs(games_year)[x])
    return teams_df

teams_with_yearly_plus_minus = yearly_team_plus_minus(teams_df, regular_season_df)
