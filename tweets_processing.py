import pandas as pd
import datetime
import matplotlib.pyplot as plt
import os
import numpy as np

# pd.set_option('display.max_columns', 10)
teams = pd.read_csv('account stats (3004).csv', header=0)

schedule = pd.read_csv("game_date_check.csv", header=0)
schedule.columns = ["date"] + schedule.columns[1:].to_list()
for i in range(schedule.shape[0]):
    schedule.iloc[i, 0] = datetime.datetime.strptime(schedule.iloc[i, 0], "%d-%b-%y")
    schedule.iloc[i, 0] = datetime.datetime.strftime(schedule.iloc[i, 0], "%Y-%m-%d")
schedule['date'] = schedule['date'].astype('str')

result = pd.read_csv("win_date_check.csv", header=0)
result.columns = ["date"] + result.columns[1:].to_list()
for i in range(result.shape[0]):
    result.iloc[i, 0] = datetime.datetime.strptime(result.iloc[i, 0], "%d-%b-%y")
    result.iloc[i, 0] = datetime.datetime.strftime(result.iloc[i, 0], "%Y-%m-%d")
result['date'] = result['date'].astype('str')

directory = os.path.join("RegularSeasonTweet_3004")
for root, dirs, files in os.walk(directory):
    for file in files:
        team_id = file[:file.find(" Tweets")]
        team_name = teams['team_name'][teams['team_id'] == team_id].item()
        print(team_name)
        column = list(schedule.columns.values)
        column.remove('date')
        column.remove(team_name)
        schedule_small = schedule.drop(columns=column)
        result_small = result.drop(columns=column)
        f = pd.read_csv("RegularSeasonTweet_3004/" + file, header=None)
        f.columns = ['comment_cnt', "id", "liked_cnt", "link", "retweet_cnt", "time", "tweeter", "text"]

        # for i in range(f.shape[0]):
        if f['comment_cnt'].dtype != np.int64:
            for i in range(f.shape[0]):
                if "千" in f['comment_cnt'].iloc[i] or "万" in f['comment_cnt'].iloc[i]:
                    if "千" in f['comment_cnt'].iloc[i]:
                        f['comment_cnt'].iloc[i] = float(f['comment_cnt'].iloc[i].replace("千", '')) * 1000
                    elif "万" in f['comment_cnt'].iloc[i]:
                        f['comment_cnt'].iloc[i] = float(f['comment_cnt'].iloc[i].replace("万", '')) * 10000
        if f['liked_cnt'].dtype != np.int64:
            for i in range(f.shape[0]):
                if "千" in f['liked_cnt'].iloc[i] or "万" in f['liked_cnt'].iloc[i]:
                    if "千" in f['liked_cnt'].iloc[i]:
                        f['liked_cnt'].iloc[i] = float(f['liked_cnt'].iloc[i].replace("千", '')) * 1000
                    elif "万" in f['liked_cnt'].iloc[i]:
                        f['liked_cnt'].iloc[i] = float(f['liked_cnt'].iloc[i].replace("万", '')) * 10000
        if f['retweet_cnt'].dtype != np.int64:
            for i in range(f.shape[0]):
                if "千" in f['retweet_cnt'].iloc[i] or "万" in f['retweet_cnt'].iloc[i]:
                    if "千" in f['retweet_cnt'].iloc[i]:
                        f['retweet_cnt'].iloc[i] = float(f['retweet_cnt'].iloc[i].replace("千", '')) * 1000
                    elif "万" in f['retweet_cnt'].iloc[i]:
                        f['retweet_cnt'].iloc[i] = float(f['retweet_cnt'].iloc[i].replace("万", '')) * 10000

        f['comment_cnt'] = f['comment_cnt'].astype(int)
        f['liked_cnt'] = f['liked_cnt'].astype(int)
        f['retweet_cnt'] = f['retweet_cnt'].astype(int)

        f[['date', 'hour']] = f.time.str.split(expand=True,)
        f['date'] = pd.to_datetime(f['date']).astype(str)
        day_mean = f[["comment_cnt", "liked_cnt", "retweet_cnt", "date"]].groupby(['date']).mean()
        day_mean.columns = ['comment_avg', 'like_avg', 'retweet_avg']
        day_max = f[["comment_cnt", "liked_cnt", "retweet_cnt", "date"]].groupby(['date']).max()
        day_max.columns = ['comment_max', 'liked_max', 'retweet_max']
        day_count = f[["comment_cnt", "date"]].groupby(['date']).count()
        day_count.columns = ['tweets_cnt']

        day_mean = day_mean.merge(day_max, left_index=True, right_index=True)
        day_mean = day_mean.merge(day_count, left_index=True, right_index=True)
        day_mean = day_mean.merge(schedule_small, how='outer', left_index=True, right_on='date')
        counts = day_mean.merge(result_small, how='outer', left_on='date', right_on='date')
        counts['has_game'] = 0
        counts['has_game'][counts[team_name+'_x'] > 0] = counts['like_avg'].max()
        counts['win'] = 0
        counts['win'][counts[team_name+'_y'] > 0] = counts['like_avg'].max()
        counts['win'][counts[team_name+'_y'] < 0] = counts['like_avg'].max()/3
        like_max_ma = counts['like_avg'].rolling(window=5).mean()
        counts_stat = pd.DataFrame(counts.describe())
        # counts_stat.to_csv('tweets_stats/'+team_name+'_stats.csv')

        # for i in range(7):
        #     plt.plot('date', counts.columns[i], data=counts)
        # plt.bar('date', 'has_game', data=counts, alpha=0.3, width=1)
        plt.bar('date', 'win', data=counts, alpha=0.3, width=1)

        plt.plot(counts['date'], like_max_ma, label='like_max_ma')
        plt.legend()
        plt.locator_params(axis='x', nbins=10)
        plt.xticks(range(1, counts.shape[0]+1), counts.date[::14])
        plt.locator_params(axis='x', nbins=counts.shape[0]/14)  # set divisor
        plt.xticks(rotation=45)

        plt.savefig('time series plots/mean/'+team_name+'.png')
        plt.show()
        plt.close()
        # counts.to_csv('tweets_stats/'+team_name+'.csv')
