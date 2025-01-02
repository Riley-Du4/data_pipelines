#connecting to servers
import pandas as pd
import numpy as np
import psycopg2
import pymssql

my_conn = psycopg2.connect(
    database="xxxx", 
    user='xxxxx', 
    password='xxxxx', 
    host='xxxx', 
    port= 'xxxxx'
)

my_cursor = my_conn.cursor()

final_conn = pymssql.connect(
    server='xxxxx',
    user='xxxxx',
    password='xxxxx',
    database='xxxxx',
    port='xxxx'
)

final_cursor = final_conn.cursor()
print("connection successfull")
# determine where to add NOT NULL

stadium_table = """
        CREATE TABLE IF NOT EXISTS stadium_table (
          stadium_id SERIAL PRIMARY KEY,
          stadium_name VARCHAR(50) NOT NULL,
          stadium_location VARCHAR(50),
          stadium_address VARCHAR(150),
          stadium_weather_station_code VARCHAR(30),
          stadium_capacity INT,
          stadium_surface VARCHAR(20),
          stadium_type VARCHAR(20),
          stadium_weather_type VARCHAR(20),
          stadium_open INT,
          stadium_close INT,
          station VARCHAR(20),
          name VARCHAR(100),
          latitude NUMERIC(10,6),
          longitude NUMERIC(10,6),
          elevation FLOAT
        );
"""

my_cursor.execute(stadium_table)
my_conn.commit()

nfl_teams = """
        CREATE TABLE IF NOT EXISTS nfl_teams (
          team_ticker SERIAL PRIMARY KEY,
          stadium_id INT NOT NULL,
          team_name VARCHAR(26) NOT NULL,
          team_name_short VARCHAR(12),
          team_id VARCHAR(3),
          team_id_pfr CHAR(3),
          team_division VARCHAR(9),
          team_division_pre2002 VARCHAR(11),
          team_conference CHAR(3) NOT NULL,
          team_conference_pre2002 CHAR(3)
        );
"""
my_cursor.execute(nfl_teams)
my_conn.commit()

game_table = """
        CREATE TABLE IF NOT EXISTS game_table (
          game_id VARCHAR(50) PRIMARY KEY,
          schedule_date DATE,
          schedule_season INT,
          schedule_week VARCHAR(20),
          schedule_playoff VARCHAR(20),
          team_home VARCHAR(50) NOT NULL,
          score_home SMALLINT NOT NULL,
          score_away SMALLINT NOT NULL,
          team_away VARCHAR(50) NOT NULL,
          team_favorite_id VARCHAR(10),
          spread_favorite NUMERIC(3,1),
          over_under_line NUMERIC(4,1),
          stadium VARCHAR(50),
          stadium_neutral VARCHAR(10),
          winner_ou VARCHAR(10) NOT NULL,
          winner_line VARCHAR(10) NOT NULL,
          weather_temperature NUMERIC(4,1),
          weather_wind_mph SMALLINT,
          weather_humidity SMALLINT,
          weather_detail VARCHAR(50)
          );
        """
my_cursor.execute(game_table)
my_conn.commit()

customers = """
        CREATE TABLE IF NOT EXISTS customers (
          customer_id SERIAL PRIMARY KEY,
          customer_fname VARCHAR(30),
          customer_lname VARCHAR(30),
          customer_age SMALLINT,
          customer_type VARCHAR(50),
          customer_since SMALLINT,
          customer_income INT,
          household_size SMALLINT,
          mode_color VARCHAR(50)
        );
        """
my_cursor.execute(customers)
my_conn.commit()

placed_bet = """
        CREATE TABLE IF NOT EXISTS placed_bet (
          bet_id SERIAL PRIMARY KEY,
          customer_id INT NOT NULL,
          game_id VARCHAR(50) NOT NULL,
          bet_amount INT,
          bet_on VARCHAR(50),
          result VARCHAR(15),
          commission INT NOT NULL
        );
        """
my_cursor.execute(placed_bet)
my_conn.commit()
f_stadiums = pd.read_csv(r"C:\Users\13097\Downloads\Data 5330 Final Project\nfl_stadiums.csv")
print(df_stadiums)

# ingesting for the stadium table

for x in df_stadiums.index:
    stadium_name = df_stadiums['stadium_name'].loc[x]
    stadium_location = df_stadiums['stadium_location'].loc[x]
    stadium_open = df_stadiums['stadium_open'].loc[x]
    stadium_close = df_stadiums['stadium_close'].loc[x]
    stadium_type = df_stadiums['stadium_type'].loc[x]
    stadium_address = df_stadiums['stadium_address'].loc[x]
    stadium_weather_station_code = df_stadiums['stadium_weather_station_code'].loc[x]
    stadium_weather_type = df_stadiums['stadium_weather_type'].loc[x]
    stadium_capacity = df_stadiums['stadium_capacity'].loc[x]
    stadium_surface = df_stadiums['stadium_surface'].loc[x]
    station = df_stadiums['STATION'].loc[x]
    name = df_stadiums['NAME'].loc[x]
    latitude = df_stadiums['LATITUDE'].loc[x]
    longitude = df_stadiums['LONGITUDE'].loc[x]
    elevation = df_stadiums['ELEVATION'].loc[x]

    # add code to clean the data
    if "Grass," in str(stadium_surface):
        stadium_surface = "Grass"
    elif str(stadium_surface)[-4:] == "Turf":
        stadium_surface = "FieldTurf"
    else:
        stadium_surface = stadium_surface

    stadium_capacity = str(stadium_capacity)
    stadium_capacity = stadium_capacity.replace(",", "")

    # could I have just tried float and int instead?
    try:
        stadium_capacity = int(stadium_capacity)
    except:
        stadium_capacity = 0
    
    if pd.isna(stadium_open):
        stadium_open = 0
    
    if pd.isna(stadium_close):
        stadium_close = 0
    stadium_open = int(stadium_open)
    stadium_close = int(stadium_close)

    latitude = pd.to_numeric(latitude)
    longitude = pd.to_numeric(longitude)
    elevation = pd.to_numeric(elevation)
    elevation = elevation.astype(float)

    my_cursor.execute(f'''
                INSERT INTO stadium_table (stadium_name, stadium_location, stadium_open, stadium_close, stadium_type, stadium_address, stadium_weather_station_code, 
                stadium_weather_type, stadium_capacity, stadium_surface, STATION, NAME, LATITUDE, LONGITUDE, ELEVATION)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    stadium_name,
                    stadium_location,
                    stadium_open,
                    stadium_close,
                    stadium_type,
                    stadium_address,
                    stadium_weather_station_code,
                    stadium_weather_type,
                    stadium_capacity,
                    stadium_surface,
                    station,
                    name,
                    latitude,
                    longitude,
                    elevation
                ))
    my_conn.commit()
    # print("Record entered into the stadium_table:", stadium_name)
print("Stadium table ingested")
# Ingesting data from nfl_teams.csv

nfl_teams_df = pd.read_csv(r"C:\Users\13097\Downloads\Data 5330 Final Project\nfl_teams.csv")

# setting validations
valid_divisions = ["AFC East", "AFC West", "AFC North", "AFC South",
                   "NFC East", "NFC West", "NFC North", "NFC South"]
valid_conferences = ["AFC", "NFC"]

# Iterate through the DataFrame
for x in nfl_teams_df.index:

    team_name = nfl_teams_df['team_name'].loc[x]
    team_name_short = nfl_teams_df['team_name_short'].loc[x]
    team_id = nfl_teams_df['team_id'].loc[x]
    team_id_pfr = nfl_teams_df['team_id_pfr'].loc[x]
    team_conference = nfl_teams_df['team_conference'].loc[x]
    team_division = nfl_teams_df['team_division'].loc[x]
    team_conference_pre2002 = nfl_teams_df['team_conference_pre2002'].loc[x]
    team_division_pre2002 = nfl_teams_df['team_division_pre2002'].loc[x]

    # Validation checks
    if team_conference not in valid_conferences:
        print(f"Warning: Invalid team_conference '{team_conference}'. Ingesting data anyway for row {x}")

    if team_division not in valid_divisions:
        print(f"Warning: Invalid team_division '{team_division}'. Ingesting data anyway for row {x}")

    # Execute the INSERT query
    my_cursor.execute(f'''
            INSERT INTO nfl_teams (
                team_name, team_name_short, team_id, team_id_pfr,
                team_conference, team_division, team_conference_pre2002,
                team_division_pre2002)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (team_name,
                    team_name_short,
                    team_id,
                    team_id_pfr,
                    team_conference,
                    team_division,
                    team_conference_pre2002,
                    team_division_pre2002
                    ))

    # Commit the transaction
    my_conn.commit()
print("Data successfully inserted into nfl_teams.")

# reading in spread_scores csv

df_spread = pd.read_csv(r"C:\Users\13097\Downloads\Data 5330 Final Project\spread_scores-2.csv")
print(df_spread)

# ingesting the games table

for x in df_spread.index:
    
    schedule_date = df_spread['schedule_date'].loc[x]
    schedule_season = df_spread['schedule_season'].loc[x]
    schedule_week = df_spread['schedule_week'].loc[x]
    schedule_playoff = df_spread['schedule_playoff'].loc[x]
    team_home = df_spread['team_home'].loc[x]
    score_home = df_spread['score_home'].loc[x]
    score_away = df_spread['score_away'].loc[x]
    team_away = df_spread['team_away'].loc[x]
    team_favorite_id = df_spread['team_favorite_id'].loc[x]
    spread_favorite = df_spread['spread_favorite'].loc[x]
    over_under_line = df_spread['over_under_line'].loc[x]
    stadium = df_spread['stadium'].loc[x]
    stadium_neutral = df_spread['stadium_neutral'].loc[x]
    weather_temperature = df_spread['weather_temperature'].loc[x]
    weather_wind_mph = df_spread['weather_wind_mph'].loc[x]
    weather_humidity = df_spread['weather_humidity'].loc[x]
    weather_detail = df_spread['weather_detail'].loc[x]

    my_cursor.execute("SELECT team_id FROM nfl_teams WHERE team_name = %s", (team_home,))
    team_id1 = my_cursor.fetchone()[0]
    my_cursor.execute("SELECT team_id FROM nfl_teams WHERE team_name = %s", (team_away,))
    team_id2 = my_cursor.fetchone()[0]

    schedule_playoff = str(schedule_playoff)
    stadium_neutral = str(stadium_neutral)

    weather_temperature = float(weather_temperature)
    try:
        weather_wind_mph = int(weather_wind_mph)
    except:
        weather_wind_mph = 0
    try:
        weather_humidity = int(weather_humidity)
    except:
        weather_humidity = 0

    schedule_season = int(schedule_season)
    spread_favorite = pd.to_numeric(spread_favorite)

    score_home = int(score_home)
    score_away = int(score_away)
    over_under_line = pd.to_numeric(over_under_line)

    total_score = score_home + score_away
    if total_score > over_under_line:
        winner_ou = "over"
    elif total_score < over_under_line:
        winner_ou = "under"
    else:
        winner_ou = "push"

    if score_home > score_away:
        winner_line = "home"
    elif score_home < score_away:
        winner_line = "away"
    else:
        winner_line = "push"

    if schedule_week == "Wildcard":
        if schedule_season <= 2020:
            schedule_wk = "18"
        elif schedule_season > 2020:
            schedule_wk = "19"
    elif schedule_week == "Division":
        if schedule_season <= 2020:
            schedule_wk = "19"
        elif schedule_season > 2020:
            schedule_wk = "20"
    elif schedule_week == "Conference":
        if schedule_season <= 2020:
            schedule_wk = "20"
        elif schedule_season > 2020:
            schedule_wk = "21"
    elif schedule_week == "Superbowl":
        if schedule_season <= 2020:
            schedule_wk = "21"
        elif schedule_season > 2020:
            schedule_wk = "22"
    elif len(schedule_week) <=1:
        schedule_wk = "0" + str(schedule_week)
    else:
        schedule_wk = str(schedule_week)
    game_id = str(schedule_season) + schedule_wk + "-" + str(team_id1) + "-" + str(team_id2)

    if ("2023" in str(game_id)) & ("LVR" in str(game_id)):
        game_id = game_id.replace("LVR", "LV")

    if ("2023" in str(game_id)) & ("JAX" in str(game_id)):
        game_id = game_id.replace("JAX", "JAC")
    
    if schedule_season >= 2015:
        my_cursor.execute(f'''
            INSERT INTO game_table(game_id, team_home, team_away, schedule_date, schedule_season, schedule_week, schedule_playoff,
            score_home, score_away, team_favorite_id, spread_favorite, over_under_line, stadium, stadium_neutral, winner_ou, winner_line, 
            weather_temperature, weather_wind_mph, weather_humidity, weather_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (game_id,
                team_home,
                team_away,
                schedule_date, 
                schedule_season, 
                schedule_week, 
                schedule_playoff,
                score_home, 
                score_away, 
                team_favorite_id, 
                spread_favorite, 
                over_under_line,
                stadium, 
                stadium_neutral, 
                winner_ou, 
                winner_line, 
                weather_temperature, 
                weather_wind_mph, 
                weather_humidity, 
                weather_detail
            ))
        my_conn.commit()
        print("Record entered into the game_table:", game_id)
    else:
        pass
print("All records ingested into game able")

# Ingesting customer table

dunn_conn = pymssql.connect(
     host='xxxx', # Server name goes in quotes.
     user='xxxx', # Username goes in quotes.
     password='xxxxx', # Password goes in quotes
     database='xxxx')
dunn_cursor = dunn_conn.cursor()

query = """ SELECT * FROM customer_table """
df = pd.read_sql(query, dunn_conn, index_col="customer_id")

team_conn = psycopg2.connect(
    database="xxxx", 
    user='xxxxx', 
    password='xxxxx', 
    host='xxxxx', 
    port= 'xxxx'
)
team_cursor = team_conn.cursor()

for x in df.index:
    name =  df['customer_name'].loc[x].split(' ')
    customer_fname = name[0]
    customer_lname = name[-1]
    customer_age = int(df['customer_age'].loc[x])
    customer_type = df['customer_type'].loc[x]
    customer_since = int(df['customer_since'].loc[x])
    customer_income = int(df['customer_income'].loc[x])
    household_size = int(df['household_size'].loc[x])
    mode_color = df['mode_color'].loc[x]
    team_cursor.execute("""INSERT INTO customers (customer_fname, customer_lname, customer_age, customer_type, customer_since, customer_income, household_size, mode_color)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (customer_fname, customer_lname, customer_age, customer_type, customer_since, customer_income, household_size, mode_color))
    team_conn.commit()
    
print("Data successfully inserted into customers.")
for x in nfl_teams_df.index:
    team_name = nfl_teams_df['team_name'].loc[x]
    team_name = str(team_name)
    my_cursor.execute("""
        SELECT max(s.stadium_id)
        FROM stadium_table s, game_table gm
        WHERE gm.stadium = s.stadium_name and gm.team_home = %s and
              gm.schedule_season = 2023 and gm.stadium_neutral = 'False';
    """, (team_name,))
    
    result = my_cursor.fetchall()
    
    if result and result[0][0] is not None:
        stadium_id = result[0][0]

        my_cursor.execute("""
            UPDATE nfl_teams
            SET stadium_id = %s
            WHERE team_name = %s;
        """, (stadium_id, team_name))
        my_conn.commit()
        
    else:
        print(f"No stadium_id found for team: {team_name}")
        
print("Stadium IDs entered into nfl_teams table.")
team_cursor = team_conn.cursor()
print("Connected to PostgreSQL (iwdm)!")

#  Extract Data from betlog in SQL Server
dunn_cursor.execute("""
    SELECT bet_id, customer_id, game_id, bet_amount, bet_on
    FROM betlog
""")
betlog_data = dunn_cursor.fetchall()
print(f"Fetched {len(betlog_data)} rows from SQL Server betlog table.")


for row in betlog_data: 
    bet_id = row[0]
    customer_id = row[1]
    game_id = row[2]
    bet_amount = row[3]
    bet_on = row[4]
    
    if bet_id >=50369:
    
        # commission calculation
        if bet_amount <= 1000:
            commission = bet_amount * 0.10
        elif bet_amount <= 5000:
            commission = 1000 * 0.10 + (bet_amount - 1000) * 0.08
        else:
            commission = 1000 * 0.10 + 4000 * 0.08 + (bet_amount - 5000) * 0.06
    
        # Fetch game details to determine the result
        team_cursor.execute("""
            SELECT score_home, score_away, spread_favorite, over_under_line, team_home, team_away
            FROM game_table
            WHERE game_id = %s
        """, (game_id,))
        game_details = team_cursor.fetchone()
    
        if game_table:
            score_home, score_away, spread_favorite, over_under_line, team_home, team_away = game_details
            total_score = score_home + score_away
    
            # Determine result based on bet type
            if bet_on == "over":
                result = "win" if total_score > over_under_line else "loss" if total_score < over_under_line else "push"
            elif bet_on == "under":
                result = "win" if total_score < over_under_line else "loss" if total_score > over_under_line else "push"
            elif bet_on == team_home:
                result = "win" if score_home - score_away > spread_favorite else "loss" if score_home - score_away < spread_favorite else "push"
            elif bet_on == team_away:
                result = "win" if score_away - score_home > -spread_favorite else "loss" if score_away - score_home < -spread_favorite else "push"
            else:
                result = "unknown"
        else:
            print(f"Warning: Game details not found for game_id {game_id}.")
            result = "unknown"
    
        # Insert data into placed_bet table
        team_cursor.execute("""
                INSERT INTO placed_bet (
                    customer_id, game_id, bet_amount, bet_on, result, commission
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                customer_id,
                game_id,
                bet_amount,
                bet_on,
                result,
                commission
            )
        )
    
        # Commit changes
        team_conn.commit()
    
    else:
        pass
print("Data successfully ingested into placed_bet.")
# game_table
query_game_table = "SELECT * FROM game_table LIMIT 5;"
df_game_table = pd.read_sql(query_game_table, my_conn)
print("game_table:")
print(df_game_table)
print("\n")

# nfl_teams table
query_nfl_teams = "SELECT * FROM nfl_teams LIMIT 5;"
df_nfl_teams = pd.read_sql(query_nfl_teams, my_conn)
print("nfl_teams:")
print(df_nfl_teams)
print("\n")

# stadium_table
query_stadium_table = "SELECT * FROM stadium_table LIMIT 5;"
df_stadium_table = pd.read_sql(query_stadium_table, my_conn)
print("stadium_table:")
print(df_stadium_table)
print("\n")

# placed_bet table
query_placed_bet = "SELECT * FROM placed_bet LIMIT 5;"
df_placed_bet = pd.read_sql(query_placed_bet, my_conn)
print("placed_bet:")
print(df_placed_bet)
print("\n")

# customer_table
query_customer_table = "SELECT * FROM customers LIMIT 5;"
df_customer_table = pd.read_sql(query_customer_table, my_conn)
print("customer_table:")
print(df_customer_table)
print("\n")

# Query 1 - customers paying over $20,000
query1 = """
SELECT
    COUNT(CASE WHEN total_commission > 20000 THEN 1 END) AS customers_over_20k,
    COUNT(*) AS total_customers,
    ROUND(COUNT(CASE WHEN total_commission > 20000 THEN 1 END) * 100.0 / COUNT(*), 2) AS percent_gift_baskets
FROM (
    SELECT
        c.customer_id,
        SUM(pb.commission) AS total_commission
    FROM
        customers c
    JOIN
        placed_bet pb ON c.customer_id = pb.customer_id
    GROUP BY
        c.customer_id
) subquery;
"""
df_summary = pd.read_sql(query1, my_conn)
print(df_summary)

# Query 2 - Top 20 customers by commission
query2 = """
SELECT
    c.customer_fname AS first_name,
    c.customer_lname AS last_name,
    SUM(pb.commission) AS total_commission
FROM
    customers c
JOIN
    placed_bet pb ON c.customer_id = pb.customer_id
GROUP BY
    c.customer_id, c.customer_fname, c.customer_lname
HAVING
    SUM(pb.commission) > 20000
ORDER BY
    total_commission DESC
LIMIT 20;
"""

df_top_customers = pd.read_sql(query2, my_conn)
print(df_top_customers)
query3= """
SELECT
    c.customer_fname AS first_name,
    c.customer_lname AS last_name,
    COUNT(pb.bet_id) AS bets_placed,
    SUM(CASE WHEN pb.result = 'win' THEN 1 ELSE 0 END) AS bets_won,
    ROUND((SUM(CASE WHEN pb.result = 'win' THEN 1 ELSE 0 END) * 100.0) / COUNT(pb.bet_id), 2) AS winning_percentage,
    SUM(CASE WHEN pb.result = 'win' THEN pb.bet_amount * 2 ELSE 0 END) AS total_amount_won
FROM
    customers c
JOIN
    placed_bet pb ON c.customer_id = pb.customer_id
GROUP BY
    c.customer_id, c.customer_fname, c.customer_lname
HAVING
    COUNT(pb.bet_id) >= 6
ORDER BY
    winning_percentage DESC, total_amount_won DESC
LIMIT 10;
"""

df3 = pd.read_sql(query3, my_conn)
print(df3)
# 4
query4 = """
SELECT
    c.customer_fname AS first_name,
    c.customer_lname AS last_name,
    COUNT(pb.bet_id) AS bets_placed,
    SUM(CASE WHEN pb.result = 'win' THEN 1 ELSE 0 END) AS bets_won,
    SUM(CASE WHEN pb.result = 'win' THEN pb.bet_amount * 2 ELSE 0 END) - SUM(pb.commission) AS net_loss
FROM
    customers c
JOIN
    placed_bet pb ON c.customer_id = pb.customer_id
GROUP BY
    c.customer_id, c.customer_fname, c.customer_lname
ORDER BY
    net_loss DESC
LIMIT 20;
"""

df4 = pd.read_sql(query4, my_conn)
print(df4)
# 5

query5 = """
SELECT
    schedule_week,
    COUNT(*) AS total_games,
    SUM(CASE WHEN sportsbook_result = 'winner' THEN 1 ELSE 0 END) AS winner_games,
    SUM(CASE WHEN sportsbook_result = 'loser' THEN 1 ELSE 0 END) AS loser_games,
    ROUND(SUM(CASE WHEN sportsbook_result = 'winner' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS percent_winners,
    ROUND(SUM(CASE WHEN sportsbook_result = 'loser' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS percent_losers
FROM (
    SELECT
        g.game_id,
        g.schedule_week,
        CASE
            WHEN SUM(pb.commission) < SUM(pb.bet_amount) THEN 'winner'
            WHEN SUM(pb.commission) >= SUM(pb.bet_amount) THEN 'loser'
        END AS sportsbook_result
    FROM
        game_table g
    JOIN
        placed_bet pb ON g.game_id = pb.game_id
    WHERE
        g.schedule_season = 2023
    GROUP BY
        g.game_id, g.schedule_week
) subquery
GROUP BY
    schedule_week
ORDER BY
    schedule_week ASC;

"""


df5 = pd.read_sql_query(query5, my_conn)
print(df5)
# 6
df6 = pd.read_sql("""
SELECT
    t.team_name,
    COUNT(DISTINCT CASE WHEN g.score_home > g.score_away AND g.team_home = t.team_name THEN g.game_id
                        WHEN g.score_away > g.score_home AND g.team_away = t.team_name THEN g.game_id END) AS num_wins,
    

    COUNT(DISTINCT CASE WHEN g.score_home < g.score_away AND g.team_home = t.team_name THEN g.game_id
                        WHEN g.score_away < g.score_home AND g.team_away = t.team_name THEN g.game_id END) AS num_losses,
    

    COUNT(DISTINCT CASE WHEN 
              ((g.team_home = t.team_name AND g.score_home > g.score_away AND pb.bet_amount < 0) OR
               (g.team_away = t.team_name AND g.score_away > g.score_home AND pb.bet_amount < 0) OR
               (g.team_home = t.team_name AND g.score_home - g.score_away > ABS(g.spread_favorite)) OR
               (g.team_away = t.team_name AND g.score_away - g.score_home > ABS(g.spread_favorite))
              ) THEN g.game_id END) AS num_beat_spread,
    
    COUNT(CASE WHEN pb.bet_on = t.team_name THEN 1 END) AS num_bets_for,
    
    COUNT(CASE 
              WHEN g.team_home = t.team_name AND pb.bet_on = g.team_away THEN 1
              WHEN g.team_away = t.team_name AND pb.bet_on = g.team_home THEN 1
        END) AS num_bets_against

FROM
    nfl_teams t
LEFT JOIN
    game_table g ON (g.team_home = t.team_name OR g.team_away = t.team_name)
LEFT JOIN
    placed_bet pb ON (pb.game_id = g.game_id)
WHERE
    g.schedule_season = 2023
GROUP BY
    t.team_name
ORDER BY
    t.team_name ASC;

""", my_conn)

print(df6)
# 7 - Pt1

import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

query = """
WITH customer_value AS (
    SELECT 
        customer_id, 
        (SUM(bet_amount) + SUM(commission) - 
         SUM(CASE 
             WHEN result = 'winner' THEN bet_amount * 2 
             WHEN result = 'push' THEN bet_amount - commission 
             ELSE 0 
         END)) AS customer_value
    FROM placed_bet
    GROUP BY customer_id
)
SELECT 
    c.customer_id, 
    c.customer_age AS customer_age,         
    c.customer_income AS customer_income,   
    c.household_size AS household_size,  
    CASE 
        WHEN c.customer_type = 'type1' THEN 1
        WHEN c.customer_type = 'type2' THEN 2
        ELSE 0 
    END AS customer_type_numeric,
    c.customer_since AS customer_since_years,
    cv.customer_value
FROM 
    customers AS c 
JOIN 
    customer_value AS cv 
ON 
    c.customer_id = cv.customer_id
"""

# Execute the query
df = pd.read_sql(query, my_conn, index_col="customer_id")

# No need for `pd.get_dummies` since customer_type is already numeric
# Check correlation matrix
matrix = df.corr()
print(matrix)

# Prepare data for regression
x = df[['customer_age', 'customer_type_numeric', 'customer_income', 'customer_since_years', 'household_size']].apply(pd.to_numeric, errors='coerce')
y = pd.to_numeric(df['customer_value'], errors='coerce')

# Add constant for statsmodels
x = sm.add_constant(x)
model = sm.OLS(y, x).fit()
print(model.summary())

#secondary analysis

reg2 = """WITH customer_value AS(SELECT customer_id, (SUM(bet_amount) + SUM(commission) - SUM(CASE WHEN result = 'winner' THEN bet_amount*2 WHEN result = 'push' THEN bet_amount-commission ELSE 0 END )) as customer_value
          FROM placed_bet
          GROUP BY customer_id)
          SELECT c.customer_id, c.customer_age, c.customer_since, c.customer_income, c.color_mode, cv.customer_value
          FROM customers as c JOIN customer_value as cv ON c.customer_id = cv.customer_id)"""
df = pd.read_sql(reg2, my_conn, index_col="customer_id")
df = pd.get_dummies(df, colums=['mode_color'], drop_first=True)
matrix = df.corr()
print(matrix)

x = df[['customer_age','customer_since','customer_income','mode_color']]
y = df['customer_value']

x = sm.add_constant(x)
model = sm.OLS(y, x).fit()
print(model.summary())
print(model.summary())

x = sm.add_constant(x)
model = sm.OLS(y, x).fit()
print(model.summary())
