import uuid

from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect, Http404
from django.core.urlresolvers import reverse

from tweets.forms import TweetForm

import riakapi

NUM_PER_PAGE = 40

def timeline(request):
    form = TweetForm(request.POST or None)
    if request.user['is_authenticated'] and form.is_valid():
        tweet_id = str(uuid.uuid1())
        riakapi.save_tweet(tweet_id, request.session['username'], {
            'username': request.session['username'],
            'body': form.cleaned_data['body'],
        })
        return HttpResponseRedirect(reverse('timeline'))
    start = request.GET.get('start')
    if request.user['is_authenticated']:
        tweets,next = riakapi.get_timeline(request.session['username'],
            start=start, limit=NUM_PER_PAGE)
    else:
        tweets,next = riakapi.get_userline(riakapi.PUBLIC_USERLINE_KEY,
            start=start, limit=NUM_PER_PAGE)
    context = {
        'form': form,
        'tweets': tweets,
        'next': next,
    }
    return render_to_response('tweets/timeline.html', context,
        context_instance=RequestContext(request))

def publicline(request):
    start = request.GET.get('start')
    tweets,next = riakapi.get_userline(riakapi.PUBLIC_USERLINE_KEY,
        start=start, limit=NUM_PER_PAGE)
    context = {
        'tweets': tweets,
        'next': next,
    }
    return render_to_response('tweets/publicline.html', context,
        context_instance=RequestContext(request))

def userline(request, username=None):
    try:
        user = riakapi.get_user_by_username(username)
    except riakapi.DatabaseError:
        raise Http404
    
    # Query for the friend ids
    friend_usernames = []
    if request.user['is_authenticated']:
        friend_usernames = riakapi.get_friend_usernames(username) + [username]
    
    # Add a property on the user to indicate whether the currently logged-in
    # user is friends with the user
    user['friend'] = username in friend_usernames
    
    start = request.GET.get('start')
    tweets,next = riakapi.get_userline(username, start=start,
        limit=NUM_PER_PAGE)
    context = {
        'user': user,
        'username': username,
        'tweets': tweets,
        'next': next,
        'friend_usernames': friend_usernames,
    }
    return render_to_response('tweets/userline.html', context,
        context_instance=RequestContext(request))
