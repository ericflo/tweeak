import time

from riak import RiakClient, RiakHttpTransport

__all__ = ['get_user_by_username', 'get_friend_usernames', 'get_publicline',
    'get_follower_usernames', 'get_users_for_usernames', 'get_friends',
    'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
    'NotFound']

CLIENT = RiakClient(host='127.0.0.1', port='8098',
    transport_class=RiakHttpTransport)


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
    if len(result) == limit:
        next = result[-1]['_ts']
        result = result[:-1]
    else:
        next = None
    return result, None

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    usernames = ', '.join(["'%s'" % u for u in get_friend_usernames(username)])
    map_func = """
function(value, keyData, arg) {
    var usernames = [%s];
    var data = Riak.mapValuesJson(value)[0];
    if(data._ts > %s && usernames.indexOf(data.username) != -1) {
        return [data];
    }
    return [];
}
    """ % (start or 0, usernames)
    reduce_func = """
function(value, arg) {
    var reverseSort = function(first, second) {
        return second - first;
    };
    return value.sort(reverseSort).slice(0, %s);
}
    """ % (limit,)
    result = CLIENT.add('tweets').map(map_func).reduce(reduce_func).run()
    return _result_next(result, limit)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    map_func = """
function(value, keyData, arg) {
    var data = Riak.mapValuesJson(value)[0];
    if(data._ts > %s && data.username === '%s') {
        return [data];
    }
    return [];
}
    """ % (start or 0, username)
    reduce_func = """
function(value, arg) {
    var reverseSort = function(first, second) {
        return second - first;
    };
    return value.sort(reverseSort).slice(0, %s);
}
    """ % (limit,)
    result = CLIENT.add('tweets').map(map_func).reduce(reduce_func).run()
    print result
    return _result_next(result, limit)

def get_publicline(start=None, limit=40):
    reduce_func = """
function(value, arg) {
    var reverseSort = function(first, second) {
        return second - first;
    };
    return value.sort(reverseSort).slice(0, %s);
}
    """ % (limit,)
    result = CLIENT.add('tweets').map('Riak.mapValuesJson')
    result = result.reduce(reduce_func).run()
    print result
    return _result_next(result, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    resp = CLIENT.bucket('tweets').get(tweet_id).get_data()
    if not resp:
        raise NotFound(tweet_id)
    return resp


# INSERTING APIs

def save_user(username, user):
    """
    Saves the user record.
    """
    CLIENT.bucket('users').new(str(username), user).store()

def save_tweet(tweet_id, username, tweet_data):
    """
    Saves the tweet record.
    """
    tweet_data['_ts'] = long(time.time() * 1e6)
    CLIENT.bucket('tweets').new(str(tweet_id), tweet_data).store()

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    following = CLIENT.bucket('following').get(from_username)
    follow_data = following.get_data() or {'usernames': []}
    follow_data['usernames'].extend(to_usernames)
    following.set_data(follow_data)
    following.store()
    
    followers_bucket = CLIENT.get('followers')
    for username in to_usernames:
        followers = followers_bucket.get(username)
        follower_data = followers.get_data() or {'usernames': []}
        follower_data['usernames'].append(from_username)
        followers.set_data(follower_data)
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
    following.store()
    
    followers_bucket = CLIENT.get('followers')
    for username in to_usernames:
        followers = followers_bucket.get(username)
        follower_data = followers.get_data() or {'usernames': []}
        follower_data['usernames'].remove(from_username)
        followers.set_data(follower_data)
        followers.store()
