import time

from riak import RiakClient, RiakHttpTransport

__all__ = ['get_user_by_username', 'get_friend_usernames', 'get_publicline',
    'get_follower_usernames', 'get_users_for_usernames', 'get_friends',
    'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
    'NotFound']

CLIENT = RiakClient(host='127.0.0.1', port='8098',
    transport_class=RiakHttpTransport)

REVERSE_SORT = """
function(value, arg) {
    var sorter = function(f, s) { 
        return (f.ts === s.ts ? 0 : (f.ts < s.ts ? 1 : -1));
    };
    return value.sort(sorter).slice(0, %s);
}
"""

class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass

class NotFound(DatabaseError):
    pass


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    data = CLIENT.bucket('users').get(username).get_data()
    if not data:
        raise NotFound(username)
    data['username'] = username
    return data

def get_friend_usernames(username):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    data = CLIENT.bucket('following').get(username).get_data()
    data = data or {'usernames': []}
    return data['usernames']

def get_follower_usernames(username):
    """
    Given a username, gets the usernames of the people following that user.
    """
    data = CLIENT.bucket('followers').get(username).get_data()
    data = data or {'usernames': []}
    return data['usernames']

def get_users_for_usernames(usernames):
    """
    Given a list of usernames, this gets the associated user object for each
    one.
    """
    return map(get_user_by_username, usernames)

def get_friends(username):
    """
    Given a username, gets the people that the user is following.
    """
    return map(get_user_by_username, get_friend_usernames(username))

def get_followers(username):
    """
    Given a username, gets the people following that user.
    """
    return map(get_user_by_username, get_follower_usernames(username))

def _result_next(result, limit):
    # Hack to detect swallowed errors:
    if 'lineno' in result and 'message' in result and 'source' in result:
        return [], None
    if len(result) == limit + 1:
        next = result[-1]['ts']
        result = result[:-1]
    else:
        next = None
    return result, next

def _get_start(start):
    if start:
        return str(start).zfill(20)
    return '9' * 20

### TODO: Determine whether map/reduce is appropriate for this situation, or
###       whether we want to create an inverted index somehow (buckets,
###       partitioned objects, or something else entirely.)

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    usernames = get_friend_usernames(username) + [username]
    usernames = ', '.join(["'%s': 1" % u for u in usernames])
    map_func = """
function(value, keyData, arg) {
    var usernames = {%s};
    var data = Riak.mapValuesJson(value)[0];
    if(data.ts <= '%s' && usernames[data.username]) {
        data.id = value.key;
        return [data];
    }
    return [];
}
    """ % (usernames, _get_start(start))
    reduce_func = REVERSE_SORT % (limit + 1,)
    result = CLIENT.add('tweets').map(str(map_func))
    result = result.reduce(str(reduce_func)).run()
    return _result_next(result, limit)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    map_func = """
function(value, keyData, arg) {
    var data = Riak.mapValuesJson(value)[0];
    if(data.ts <= '%s' && data.username === '%s') {
        data.id = value.key;
        return [data];
    }
    return [];
}
    """ % (_get_start(start), username)
    reduce_func = REVERSE_SORT % (limit + 1,)
    result = CLIENT.add('tweets').map(str(map_func))
    result = result.reduce(str(reduce_func)).run()
    return _result_next(result, limit)

def get_publicline(start=None, limit=40):
    map_func = """
function(value, keyData, arg) {
    var data = Riak.mapValuesJson(value)[0];
    if(data.ts <= '%s') {
        data.id = value.key;
        return [data];
    }
    return [];
}
    """ % (_get_start(start),)
    reduce_func = REVERSE_SORT % (limit + 1,)
    result = CLIENT.add('tweets').map(str(map_func))
    result = result.reduce(str(reduce_func)).run()
    return _result_next(result, limit)

def get_tweet(tweetid):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    resp = CLIENT.bucket('tweets').get(tweetid).get_data()
    if not resp:
        raise NotFound(tweetid)
    resp['id'] = tweetid
    return resp


# INSERTING APIs

def save_user(username, user):
    """
    Saves the user record.
    """
    CLIENT.bucket('users').new(str(username), user).store()

def save_tweet(tweetid, username, tweet_data):
    """
    Saves the tweet record.
    """
    tweet_data['ts'] = str(long(time.time() * 1e6)).zfill(20)
    CLIENT.bucket('tweets').new(str(tweetid), tweet_data).store()

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    following = CLIENT.bucket('following').get(from_username)
    follow_data = following.get_data() or {'usernames': []}
    follow_data['usernames'] = list(set(
        follow_data['usernames'] + to_usernames))
    following.set_data(follow_data)
    following.set_content_type('application/json')
    following.store()
    
    followers_bucket = CLIENT.bucket('followers')
    for username in to_usernames:
        followers = followers_bucket.get(username)
        follower_data = followers.get_data() or {'usernames': []}
        if from_username not in follower_data['usernames']:
            follower_data['usernames'].append(from_username)
        followers.set_data(follower_data)
        followers.set_content_type('application/json')
        followers.store()

def remove_friends(from_username, to_usernames):
    """
    Removes a friendship relationship from one user to some others.
    """
    following = CLIENT.bucket('following').get(from_username)
    follow_data = following.get_data() or {'usernames': []}
    for username in to_usernames:
        follow_data['usernames'].remove(username)
    following.set_data(follow_data)
    following.set_content_type('application/json')
    following.store()
    
    followers_bucket = CLIENT.bucket('followers')
    for username in to_usernames:
        followers = followers_bucket.get(username)
        follower_data = followers.get_data() or {'usernames': []}
        try:
            follower_data['usernames'].remove(from_username)
        except ValueError:
            continue
        followers.set_data(follower_data)
        followers.set_content_type('application/json')
        followers.store()
