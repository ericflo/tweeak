
__all__ = ['get_user_by_username', 'get_friend_usernames',
    'get_follower_usernames', 'get_users_for_usernames', 'get_friends',
    'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
    'NotFound', 'PUBLIC_USERLINE_KEY']

PUBLIC_USERLINE_KEY = '!PUBLIC!'

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
    pass

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    pass

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    pass

def get_users_for_usernames(usernames):
    """
    Given a list of usernames, this gets the associated user object for each
    one.
    """
    pass

def get_friends(username, count=5000):
    """
    Given a username, gets the people that the user is following.
    """
    pass

def get_followers(username, count=5000):
    """
    Given a username, gets the people following that user.
    """
    pass

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    pass

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    pass

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    pass


# INSERTING APIs

def save_user(username, user):
    """
    Saves the user record.
    """
    pass

def save_tweet(tweet_id, username, tweet):
    """
    Saves the tweet record.
    """
    pass

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    pass

def remove_friends(from_username, to_usernames):
    """
    Removes a friendship relationship from one user to some others.
    """
    pass
