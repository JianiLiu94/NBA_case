import pandas as pd

# raw data comes from the source code of
# https://e.infogram.com/00be5c6a-02c5-4e15-8923-143d7a9a4713?referrer=https%3A%2F%2Fwww.forbes.com%2Fsites%2Fkurtbadenhausen%2F2019%2F02%2F06%2Fnba-team-values-2019-knicks-on-top-at-4-billion%2F&src=embed
# which is embeded on the Forbes article
# https://www.forbes.com/sites/kurtbadenhausen/2019/02/06/nba-team-values-2019-knicks-on-top-at-4-billion/#33ca812fe667
data_raw = [
[["","Sport","Market","Stadium","Brand"],["New York Knicks","611","1831","995","563"],["Los Angeles Lakers","587","1866","573","674"],["Golden State Warriors","633","1376","884","606"],["Chicago Bulls","1015","965","604","316"],["Boston Celtics","1051","978","433","337"],["Brooklyn Nets","830","732","577","211"],["Houston Rockets","668","848","530","254"],["Dallas Mavericks","850","785","360","254"],["Los Angeles Clippers","927","756","283","235"],["Miami Heat","741","521","301","186"],["Toronto Raptors","654","472","384","164"],["Philadelphia 76ers","694","505","265","186"],["San Antonio Spurs","679","465","346","135"],["Portland Trail Blazers","760","409","279","152"],["Sacramento Kings","675","419","312","169"],["Washington Wizards","685","453","258","154"],["Phoenix Suns","783","351","245","121"],["Oklahoma City Thunder","725","388","231","131"],["Utah Jazz","718","360","231","116"],["Indiana Pacers","816","258","232","94"],["Denver Nuggets","785","281","203","106"],["Milwaukee Bucks","889","250","115","95"],["Orlando Magic","751","314","150","109"],["Atlanta Hawks","727","283","180","110"],["Cleveland Cavaliers","449","415","244","166"],["Detroit Pistons","634","373","155","107"],["Minnesota Timberwolves","727","300","139","94"],["Charlotte Hornets","777","221","176","76"],["New Orleans Pelicans","767","215","152","85"],["Memphis Grizzlies","780","245","110","66"]],
[["","Revenue Per Fan"],["New York Knicks","37"],["Los Angeles Lakers","51"],["Golden State Warriors","47"],["Chicago Bulls","20"],["Boston Celtics","39"],["Brooklyn Nets","19"],["Houston Rockets","37"],["Dallas Mavericks","25"],["Los Angeles Clippers","23"],["Miami Heat","24"],["Toronto Raptors","28"],["Philadelphia 76ers","25"],["San Antonio Spurs","61"],["Portland Trail Blazers","54"],["Sacramento Kings","68"],["Washington Wizards","23"],["Phoenix Suns","24"],["Oklahoma City Thunder","88"],["Utah Jazz","101"],["Indiana Pacers","46"],["Denver Nuggets","33"],["Milwaukee Bucks","44"],["Orlando Magic","40"],["Atlanta Hawks","16"],["Cleveland Cavaliers","93"],["Detroit Pistons","27"],["Minnesota Timberwolves","26"],["Charlotte Hornets","32"],["New Orleans Pelicans","61"],["Memphis Grizzlies","53"]],
[["","Total Value"],["New York Knicks","4000"],["Los Angeles Lakers","3700"],["Golden State Warriors","3500"],["Chicago Bulls","2900"],["Boston Celtics","2800"],["Brooklyn Nets","2400"],["Houston Rockets","2300"],["Dallas Mavericks","2300"],["Los Angeles Clippers","2200"],["Miami Heat","1800"],["Toronto Raptors","1700"],["Philadelphia 76ers","1700"],["San Antonio Spurs","1600"],["Portland Trail Blazers","1600"],["Sacramento Kings","1600"],["Washington Wizards","1600"],["Phoenix Suns","1500"],["Oklahoma City Thunder","1500"],["Utah Jazz","1400"],["Indiana Pacers","1400"],["Denver Nuggets","1400"],["Milwaukee Bucks","1400"],["Orlando Magic","1300"],["Atlanta Hawks","1300"],["Cleveland Cavaliers","1300"],["Detroit Pistons","1300"],["Minnesota Timberwolves","1300"],["Charlotte Hornets","1300"],["New Orleans Pelicans","1200"],["Memphis Grizzlies","1200"]]
]

# check if team names are always in the same order
# for i in range(30):
#     if data_raw[0][i+1][0] != data_raw[1][i+1][0] or data_raw[0][i+1][0] != data_raw[2][i+1][0]:
#         print(i+1)

# restructure
rev_data = {'team': [], 'sport': [], 'market': [], 'stadium': [], 'brand': [], 'rev_per_fan': [], 'rev_total': []}
for i in range(30):
    rev_data['team'].append(data_raw[0][i+1][0])
    rev_data['sport'].append(data_raw[0][i+1][1])
    rev_data['market'].append(data_raw[0][i+1][2])
    rev_data['stadium'].append(data_raw[0][i+1][3])
    rev_data['brand'].append(data_raw[0][i+1][4])
    rev_data['rev_per_fan'].append(data_raw[1][i+1][1])
    rev_data['rev_total'].append(data_raw[2][i+1][1])

rev_data = pd.DataFrame(data=rev_data,
                        columns=['team', 'sport', 'market', 'stadium', 'brand', 'rev_per_fan', 'rev_total'])
rev_data.to_csv('nba_profit/forbes_valuation_data_2019.csv')
