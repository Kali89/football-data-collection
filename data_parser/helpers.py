import json
import psycopg2
import datetime


def yield_file(filename):
    with open(filename, 'rb') as f:
        for jsonline in f:
            yield(json.loads(jsonline))


def convert_datetime(time):
    dt = datetime.datetime.fromtimestamp(time / 1000.)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_odd_group(event):
    event_list = []
    id = event['id']
    ltp = event['ltp']
    tv = event['tv']
    if 'batb' in event:
        for entry in event['batb']:
            sort_index, odds, vol = entry
            event_list.append({
                'id': id,
                'ltp': ltp,
                'tv': tv,
                'odd_type': 'batb',
                'sort_index': sort_index,
                'odds': odds,
                'vol': vol,
                'prev_price': None,
                'new_price': None
            })
    elif 'batl' in event:
        for entry in event['batl']:
            sort_index, odds, vol = entry
            event_list.append({
                'id': id,
                'ltp': ltp,
                'tv': tv,
                'odd_type': 'batl',
                'sort_index': sort_index,
                'odds': odds,
                'vol': vol,
                'prev_price': None,
                'new_price': None
            })
    elif 'trd' in event:
        for entry in event['trd']:
            previous_price, new_price = entry
            event_list.append({
                'id': id,
                'ltp': ltp,
                'tv': tv,
                'odd_type': 'trd',
                'sort_index': None,
                'odds': None,
                'vol': None,
                'prev_price': previous_price,
                'new_price': new_price
            })

    else:
        print(event)
    return event_list


def parse_odds(event, ts):
    event_change_datetime = ts
    mc_id = event['id']
    event_list = [parse_odd_group(e) for e in event['rc']]
    return {
        'event_change_datetime': event_change_datetime,
        'mc_id': mc_id,
        'odds': event_list
    }


def parse_runners(runners):
    runners_list = []
    for runner in runners:
        runners_list.append(
            {
                'status': runner['status'],
                'sort_priority': runner['sortPriority'],
                'id': runner['id'],
                'name': runner['name']
            }
        )
    return runners_list


def parse_market_event(event, ts):
    event_change_datetime = ts
    id = event['id']
    real_event = event['marketDefinition']
    event_id = real_event.get('eventId', None)
    event_type_id = real_event.get('eventTypeId', None)
    betting_type = real_event.get('bettingType', None)
    market_type = real_event.get('marketType', None)
    market_time = real_event.get('marketTime', None)
    suspend_time = real_event.get('suspendTime', None)
    complete = real_event.get('complete', None)
    in_play = real_event.get('inPlay', None)
    bet_delay = real_event.get('betDelay', None)
    status = real_event.get('status', None)
    country_code = real_event.get('countryCode', None)
    open_date = real_event.get('openDate', None)
    name = real_event.get('name', None)
    event_name = real_event.get('eventName', None)
    runners = parse_runners(real_event['runners'])
    return {
       'event_change_datetime': event_change_datetime,
       'id': id,
       'event_id': event_id,
       'event_type_id': event_type_id,
       'betting_type': betting_type,
       'market_type': market_type,
       'market_time': market_time,
       'suspend_time': suspend_time,
       'complete': complete,
       'in_play': in_play,
       'bet_delay': bet_delay,
       'status': status,
       'country_code': country_code,
       'open_date': open_date,
       'name': name,
       'event_name': event_name,
       'runners': runners
    }


def parse_tv(event, ts):
    id = event['id']
    tv = event['tv']
    return {
        'event_change_datetime': ts,
        'id': id,
        'tv': tv
    }


def get_filter_games(filename):
    game_list = []
    with open(filename, 'r') as f:
        for line in f:
            num, game, other = line.split('"')
            game_list.append(game.rstrip('"'))
    return game_list


def insert_market(entry, cur):
    command_base = """
    INSERT INTO score_market_changes(change_time, mc_id, event_id, event_type_id, betting_type, market_type, market_time, suspend_time, complete, in_play, bet_delay, status, country_code, open_date, name, event_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    runners_base = """
    INSERT INTO score_runners(id, mc_id, status, sort_priority, rc_id, name) VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(
        command_base,
        (
            datetime.datetime.strptime(
                entry['event_change_datetime'], "%Y-%m-%d %H:%M:%S"
            ),
            entry['id'],
            entry['event_id'],
            entry['event_type_id'],
            entry['betting_type'],
            entry['market_type'],
            datetime.datetime.strptime(
                entry['market_time'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
            ),
            datetime.datetime.strptime(
                entry['suspend_time'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
            ),
            entry['complete'],
            entry['in_play'],
            entry['bet_delay'],
            entry['status'],
            entry['country_code'],
            datetime.datetime.strptime(
                entry['open_date'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
            ),
            entry['name'],
            entry['event_name']
        )
    )
    new_id = cur.fetchone()[0]
    for runner in entry['runners']:
        cur.execute(
            runners_base,
            (
                new_id,
                entry['id'],
                runner['status'],
                runner['sort_priority'],
                runner['id'],
                runner['name']
            )
        )


def insert_odds(entry, cur):
    command_base = "INSERT INTO score_odds_changes(change_time, mc_id, rc_id, ltp, odd_type, prev_price, new_price, odds, sort_index, vol, tv) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mc_id = entry['mc_id']
    ts = entry['event_change_datetime']
    for odd in entry['odds']:
        for ind in odd:
            rc_id = ind['id']
            ltp = ind['ltp']
            new_price = ind['new_price']
            odd_type = ind['odd_type']
            odds = ind['odds']
            prev_price = ind['prev_price']
            sort_index = ind['sort_index']
            tv = ind['tv']
            vol = ind['vol']
            cur.execute(
                command_base,
                (
                    ts,
                    mc_id,
                    rc_id,
                    ltp,
                    odd_type,
                    prev_price,
                    new_price,
                    odds,
                    sort_index,
                    vol,
                    tv
                )
            )


def insert_tv(entry, cur):
    command_base = "INSERT INTO score_tv_tracker(change_time, mc_id, tv) VALUES(%s, %s, %s)"
    mc_id = entry['id']
    ts = entry['event_change_datetime']
    tv = entry['tv']
    cur.execute(
        command_base,
        (
            ts,
            mc_id,
            tv
        )
    )


def new_solution(filename):
    i = 0
    gen = yield_file(filename)
    conn = psycopg2.connect("dbname=football")
    cur = conn.cursor()
    for event in gen:
        i += 1
        ts = convert_datetime(event['pt'])
        interesting = event['mc'][0]
        if 'marketDefinition' in interesting:
            market_dict = parse_market_event(interesting, ts)
            insert_market(market_dict, cur)
        elif 'rc' in interesting:
            odds_dict = parse_odds(interesting, ts)
            insert_odds(odds_dict, cur)
        else:
            tv_dict = parse_tv(interesting, ts)
            insert_tv(tv_dict, cur)
        if i % 100 == 0:
            conn.commit()
        if i % 1000 == 0:
            print(i)
    conn.commit()
    cur.close()
    conn.close()


def insert_odds_file(filename):
    conn = psycopg2.connect("dbname=football")
    cur = conn.cursor()
    odds_gen = yield_file(filename)
    command_base = "INSERT INTO odds_changes(change_time, mc_id, rc_id, ltp) VALUES(%s, %s, %s, %s)"
    i = 0
    for entry in odds_gen:
        cur.execute(command_base, (
            datetime.datetime.strptime(entry['event_change_datetime'], "%Y-%m-%d %H:%M:%S"),
            entry['mc_id'],
            entry['rc_id'],
            entry['ltp']
        ))
        i += 1
        if i % 10000 == 0:
            print(i)
    conn.commit()
    cur.close()
    conn.close()


def insert_market_def(market_file):
    gen = yield_file(market_file)
    conn = psycopg2.connect("dbname=football")
    cur = conn.cursor()
    i = 0
    command_base = """
    INSERT INTO market_changes(change_time, mc_id, event_id, event_type_id, betting_type, market_type, market_time, suspend_time, complete, in_play, bet_delay, status, country_code, open_date, name, event_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    runners_base = """
    INSERT INTO runners(id, mc_id, status, sort_priority, rc_id, name) VALUES (%s, %s, %s, %s, %s, %s)
    """
    for entry in gen:
        i += 1
        if i % 1000 == 1:
            print(i)
        cur.execute(
            command_base,
            (
                datetime.datetime.strptime(
                    entry['event_change_datetime'], "%Y-%m-%d %H:%M:%S"
                ),
                entry['id'],
                entry['event_id'],
                entry['event_type_id'],
                entry['betting_type'],
                entry['market_type'],
                datetime.datetime.strptime(
                    entry['market_time'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
                ),
                datetime.datetime.strptime(
                    entry['suspend_time'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
                ),
                entry['complete'],
                entry['in_play'],
                entry['bet_delay'],
                entry['status'],
                entry['country_code'],
                datetime.datetime.strptime(
                    entry['open_date'].split('.')[0], "%Y-%m-%dT%H:%M:%S"
                ),
                entry['name'],
                entry['event_name']
            )
        )
        new_id = cur.fetchone()[0]
        for runner in entry['runners']:
            cur.execute(
                runners_base,
                (
                    new_id,
                    entry['id'],
                    runner['status'],
                    runner['sort_priority'],
                    runner['id'],
                    runner['name']
                )
            )
    conn.commit()
    cur.close()
    conn.close()


def split_into_files(filename):
    gen = yield_file(filename)
#    game_list = get_filter_games('smaller_list.txt')
    with open('market_definitions.json', 'w') as f:
        with open('odds_changes.json', 'w') as g:
            for event in gen:
                if 'rc' in event['mc'][0]:
                    odds_dict = parse_odds(event)
                    g.write(json.dumps(odds_dict) + '\n')
                else:
                    market_dict = parse_market_event(event)
#                    if market_dict['event_name'] in game_list:
                    f.write(json.dumps(market_dict) + '\n')


def filter_odds(odds_file, market_change_file):
    markets_gen = yield_file(market_change_file)
    odds_gen = yield_file(odds_file)
    unique_markets = set(entry['id'] for entry in markets_gen)
    with open('filtered_odds.json', 'w') as f:
        for odd in odds_gen:
            if odd['mc_id'] in unique_markets:
                f.write(json.dumps(odd) + '\n')


NEW_ODDS_TABLE_CREATION = """
    CREATE TABLE score_odds_changes (
        id SERIAL PRIMARY KEY,
        change_time TIMESTAMP NOT NULL,
        mc_id VARCHAR(255) NOT NULL,
        rc_id INTEGER NOT NULL,
        ltp NUMERIC NOT NULL,
        odd_type VARCHAR(255) NOT NULL,
        prev_price INTEGER,
        new_price INTEGER,
        odds NUMERIC,
        sort_index INTEGER NOT NULL,
        vol NUMERIC,
        tv NUMERIC
    );
"""

ODDS_TABLE_CREATION = """
    CREATE TABLE odds_changes (
        id SERIAL PRIMARY KEY,
        change_time TIMESTAMP NOT NULL,
        mc_id VARCHAR(255) NOT NULL,
        rc_id INTEGER NOT NULL,
        ltp NUMERIC NOT NULL
    )
"""

MARKET_TABLE_CREATION = """
    CREATE TABLE market_changes (
     id SERIAL PRIMARY KEY,
     change_time TIMESTAMP NOT NULL,
     mc_id VARCHAR(255) NOT NULL,
     event_id INTEGER NOT NULL,
     event_type_id INTEGER NOT NULL,
     betting_type VARCHAR(255) NOT NULL,
     market_type VARCHAR(255),
     market_time TIMESTAMP,
     suspend_time TIMESTAMP,
     complete BOOLEAN,
     in_play BOOLEAN,
     bet_delay INTEGER,
     status VARCHAR(255),
     country_code VARCHAR(255),
     open_date TIMESTAMP,
     name VARCHAR(255),
     event_name VARCHAR(255)
     )
     """

RUNNERS_TABLE_CREATION = """
    CREATE TABLE runners (
         base_id SERIAL PRIMARY KEY,
         id VARCHAR(255),
         mc_id VARCHAR(255),
         status VARCHAR(255),
         sort_priority INTEGER,
         rc_id INTEGER,
         name VARCHAR(255)
         )
    """



