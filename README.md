# Tweeak

Tweeak is an example project, created to learn and demonstrate how to use
Riak.  Running the project will present a website that has similar
functionality to Twitter.

Most of the magic happens in tweeak/riakapi.py, so check that out.

## Installation

Installing Tweeak is fairly straightforward.  Really it just involves
checking out Riak and Tweeak, doing a little configuration, and
then starting it up.  Here's a roadmap of the steps we're going to take to
install the project:

1. Install Riak
2. Check out the Tweeak source code
3. Create a virtual Python environment with Tweeak's dependencies
4. Start up the webserver

### Install Riak

Thankfully, the Riak documentation on installation are quite good, so follow
the instructions [there](https://wiki.basho.com/display/RIAK/Installation+and+Setup).

### Check out the Tweeak source code

git clone git://github.com/ericflo/tweeak.git

### Create a virtual Python environment

First, make sure to have virtualenv installed.  If it isn't installed already,
this should do the trick:

    sudo easy_install -U virtualenv

Now let's create a new virtual environment, and begin using it:

    virtualenv twk
    source twk/bin/activate

We should install pip, so that we can more easily install Tweeak's
dependencies into our new virtual environment:

    easy_install -U pip

Now let's install all of the dependencies:

    pip install -U -r tweeak/requirements.txt

Now that we've got all of our dependencies installed, we're ready to start up
the server.

### Start up the webserver

This is the fun part! We're done setting everything up, we just need to run it:

    python manage.py runserver

Now go to http://127.0.0.1:8000/ and you can play with Tweeak!
