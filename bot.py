import argparse
import time
import common
import sqlite3
import pprint
import urllib
import json
import mastodon

def call_twitch_api(url, query_dict, twitch_client_id):
    query = urllib.parse.urlencode(query_dict)
    
    u = urllib.parse.urlparse(url)
    u = urllib.parse.ParseResult(u.scheme, u.netloc, u.path, u.params, query, u.fragment)
    u = u.geturl()
    
    req = urllib.request.Request(u, headers={'Client-ID':twitch_client_id}, method='GET')
    resp = urllib.request.urlopen(req)
    assert(resp.getcode() == 200)
    b = resp.read(resp.length)
    resp.close()

    j = json.loads(b.decode('UTF-8'))
    return j

def get_game_name_from_db(game_id, db_conn):
    cursor = db_conn.execute('SELECT game_name FROM game WHERE game_id = ?',(game_id,))
    d = cursor.fetchall()
    cursor.close()
    if len(d) != 1:
        return None
    return d[0][0]

def get_game_name_from_twitch(game_id, twitch_client_id):
    j = call_twitch_api(
        'https://api.twitch.tv/helix/games',
        {'id':game_id},
        twitch_client_id
    )
    return j['data'][0]['name']

def set_game_name_to_db(game_id, game_name, last_seen, db_conn):
    if get_game_name_from_db(game_id, db_conn) is not None:
        db_conn.execute( \
            'UPDATE game SET game_name = ?, last_seen = ? WHERE game_id = ?',
            (game_name, last_seen, game_id)
        ).close()
    else:
        db_conn.execute( \
            'INSERT INTO game (game_id, game_name, last_seen) VALUES (?,?,?)',
            (game_id, game_name, last_seen)
        ).close()

def prepare_db(db_conn):
    cursor = db_conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stream
        (
            stream_id INTEGER PRIMARY KEY ASC,
            last_seen INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS game
        (
            game_id INTEGER PRIMARY KEY ASC,
            game_name TEXT,
            last_seen INTEGER
        )
    ''')
    cursor.close()

def get_stream_list(twitch_user_login, twitch_client_id):
    j = call_twitch_api(
        'https://api.twitch.tv/helix/streams',
        {'user_login':twitch_user_login},
        twitch_client_id
    )
    return j['data']

def memory_exist(stream, db_conn):
    c = db_conn.execute( \
        'SELECT count(1) FROM stream WHERE stream_id = ?', \
        (stream['id'],)
    )
    cnt = c.fetchall()[0][0]
    c.close()
    return cnt>0

def get_game_name(game_id, db_conn, twitch_client_id, timestamp):
    game_name = get_game_name_from_db(game_id, db_conn)
    if game_name is not None:
        return game_name
    game_name = get_game_name_from_twitch(game_id, twitch_client_id)
    set_game_name_to_db(game_id, game_name, timestamp, db_conn)
    return game_name

def create_mastodon(mastodon_account):
    return mastodon.Mastodon( \
        api_base_url = mastodon_account['api_base_url'], \
        client_id = mastodon_account['client_id'], \
        client_secret = mastodon_account['client_secret'], \
        access_token = mastodon_account['access_token'] \
    )

def create_stream_toot(stream, mtd, toot_param_dict):
    mtd.status_post(
        status=toot_param_dict['status_format'].format(**stream),
        sensitive=toot_param_dict['sensitive'],
        spoiler_text=toot_param_dict['spoiler_text'],
        visibility=toot_param_dict['visibility']
    )

def update_db_stream_list(db_conn, stream_list, last_seen):
    for stream in stream_list:
        if memory_exist(stream, db_conn):
            db_conn.execute( \
                'UPDATE stream SET last_seen = ? WHERE stream_id = ?',
                (last_seen, stream['id'])
            ).close()
        else:
            db_conn.execute( \
                'INSERT INTO stream (stream_id,last_seen) VALUES (?,?)',
                (stream['id'], last_seen)
            ).close()

def forget_db_stream_list(db_conn, last_seen):
    db_conn.execute( \
        'DELETE FROM stream WHERE last_seen < ?',
        (last_seen,)
    ).close()

def forget_db_game_list(db_conn, last_seen):
    db_conn.execute( \
        'DELETE FROM game WHERE last_seen < ?',
        (last_seen,)
    ).close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('--test', action='store_true')
    args = parser.parse_args()

    timestamp = int(time.time())
    config = common.read_json(args.filename)
    assert(config is not None)

    db_conn = sqlite3.connect(config['database_file'])
    prepare_db(db_conn)

    stream_list = get_stream_list(config['twitch_user_login'],config['twitch_client_id'])

    # filter new stream
    new_stream_list = stream_list
    new_stream_list = filter(lambda stream: not memory_exist(stream, db_conn), new_stream_list)
    new_stream_list = list(new_stream_list)

    # get game name from id
    for stream in new_stream_list:
        stream['game_name'] = get_game_name(stream['game_id'], db_conn, config['twitch_client_id'], timestamp)

    # put url
    for stream in new_stream_list:
        stream['stream_url'] = 'https://www.twitch.tv/{0}'.format(config['twitch_user_login'])

    # in test, print new_stream_list
    if args.test:
        pprint.pprint(new_stream_list)

    # in non-test, toot new_stream_list
    if (not args.test) and (len(new_stream_list) > 0):
        mtd = create_mastodon(config['mastodon_account'])
        for stream in new_stream_list:
            create_stream_toot(stream, mtd, config['toot_param_dict'])

    # remember new thing
    update_db_stream_list(db_conn, stream_list, timestamp)
    
    # forget old thing
    forget_db_stream_list(db_conn, timestamp-config['stream_memory_sec'] )
    forget_db_game_list(db_conn, timestamp-config['game_memory_sec'] )
    
    # write to db, close
    db_conn.commit()
    db_conn.close()
    