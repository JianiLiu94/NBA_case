import pandas as pd

teams = pd.read_csv("account stats (3004).csv", header=0)

games = pd.read_csv('schedule_regular_season_201819.csv', header=0, thousands=',')

games['home_win'] = 1
games['home_win'][games['PTS'] > games['PTS.1']] = 0

home_win = games[['Home/Neutral', 'home_win']].groupby(['Home/Neutral']).sum()
visit_lose = games[['Visitor/Neutral', 'home_win']].groupby(['Visitor/Neutral']).sum()
home_attend_mean = games[['Home/Neutral', 'Attend.']].groupby(['Home/Neutral']).mean()
visit_attend_mean = games[['Visitor/Neutral', 'Attend.']].groupby(['Visitor/Neutral']).mean()
home_attend_max = games[['Home/Neutral', 'Visitor/Neutral', 'Attend.']].loc[games[['Home/Neutral', 'Attend.']].reset_index().groupby(['Home/Neutral'])['Attend.'].idxmax()]
visit_attend_max = games[['Visitor/Neutral', 'Home/Neutral', 'Attend.']].loc[games[['Visitor/Neutral', 'Attend.']].reset_index().groupby(['Visitor/Neutral'])['Attend.'].idxmax()]

stats = teams.merge(home_win, left_on='team_name', right_index=True)
stats = stats.merge(visit_lose, left_on='team_name', right_index=True)
stats['visit_win'] = 42 - stats['home_win_y']
stats = stats.merge(home_attend_max, left_on='team_name', right_on='Home/Neutral')
stats = stats.merge(visit_attend_max, left_on='team_name', right_on='Visitor/Neutral')
stats = stats.merge(home_attend_mean, left_on='team_name', right_index=True)
stats = stats.merge(visit_attend_mean, left_on='team_name', right_index=True)
stats.columns = ['team_name', "account_name", "team_id", "team_city", "link", "liked_cnt", "follower_cnt",
                 'nnn', 'home_win', 'visit_lose', 'visit_win', 'xxx', 'home_max_attend_rival', 'home_max_attend',
                 'yyy', 'visit_max_attend_rival', "visit_max_attend", "home_attend_mean", "visit_attend_mean"]

stats.to_csv('C:/Users/Jiani Liu/PycharmProjects/nba_profit/teams_statistics.csv')