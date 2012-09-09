wor#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
topsy-tweet-retriever.py
----------
A Topsy tweet retriever. Catches tweets from Topsy by ID, then uses Twitter API
to get complete JSON data of old tweets and stores them in a local RDB.

Coded by amp
'''

import twitter
import pycurl
import StringIO
import cStringIO
from HTMLParser import HTMLParser
import time
import _mysql
import _mysql_exceptions
import MySQLdb
import json
import simplejson
import time
from datetime import date
from django.utils.encoding import smart_str, smart_unicode
import re
import sys
import getopt
import urllib2

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def main():
    q = ""
    s = ""
    e = ""
    o = ""

    # -2. Preprocess program arguments
    optlist, args = getopt.getopt(sys.argv[1:], "hq:s:e:o:", ["help", "query=", "stardate=", "enddate=", "order="])

    for a in optlist:
        if a[0] == "-q" or a[0] == "--query":
            q = "%23" + a[1].replace(" ", "+%23")
        if a[0] == "-s" or a[0] == "--startdate":
            s = date(int(a[1].split("-")[0]), int(a[1].split("-")[1]), int(a[1].split("-")[2]))
        if a[0] == "-e" or a[0] == "--enddate":
            e = date(int(a[1].split("-")[0]), int(a[1].split("-")[1]), int(a[1].split("-")[2]))
        if a[0] == "-o" or a[0] == "--order":
            o = a[1]



    # print timedif.days

    # sys.exit(1)

    # -1. Create a subclass for HTML parser and override the handler methods

    class catchHTMLParser(HTMLParser):
        tweet_ids = set()

        def handle_starttag(self, tag, attrs):
            if tag == "a" and len(attrs) > 0 and attrs[0][1].find("/status/") >= 0 and attrs[0][1].split("/")[-1] not in self.tweet_ids:
                self.tweet_ids.add(int(attrs[0][1].split("/")[-1]))


    # 0. Parameters -- Topsy parameters and DB connection

    basetime = 1304200800 # Base time for the date-based search: 01/05/2011 00:00
    basedate = date(2011, 5, 1)
    timediff = (s - basedate).days * 24 * 3600
    starttime = basetime + timediff
    offset = 0 # Pagination of results (10 results per HTML GET)
    ids_set = set()
    ids = []
    hours = (e - s).days * 24 # How many hours of tweets to retrieve

    host = "localhost"
    user = "censorship"
    passwd = "censorship"
    name = "censorship"

    # 1. Get HTML from Topsy according to parameters, and parse HTML

    print "Retrieving ids to fetch from Topsy, please wait."

    c = pycurl.Curl()
    parser = catchHTMLParser()

    for hour in range(hours):
        offset = 0
        while True:
            topsy_url = "http://www.topsy.com/s?order=" + o + "&type=tweet&mintime=" + str(starttime + (hour * 3600)) + "&maxtime=" + str(starttime + ((hour + 1) * 3600)) + "&q=" + q + "&offset=" + str(offset)
            print topsy_url
            c.setopt(pycurl.URL, topsy_url)
                        
            b = StringIO.StringIO()
            c.setopt(pycurl.WRITEFUNCTION, b.write)
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.perform()
            html = b.getvalue()

            # Parse HTML

            parser.feed(html)
            # print catchHTMLParser.tweet_ids
            if len(catchHTMLParser.tweet_ids) == 0:
                break
            ids_set.update(catchHTMLParser.tweet_ids)
            catchHTMLParser.tweet_ids = set()
            offset = offset + 10

            ids = list(ids_set)
            print len(ids)
    
    # 2. Get tweets from Twitter and save to RDB
    print len(ids)
    # sys.exit("Just wanted to count without altering DB, exiting.")
            
    db = MySQLdb.connect(host, user, passwd, name, use_unicode=True, charset="utf8")
    db.escape_string("'")
    c = db.cursor()

    api = twitter.Api(consumer_key='w0rNaAwuWeNAnM7H3iEw', consumer_secret='BhZ3H5jZjKHrfA1nKpwGTKa959h3cNFuSpRiKEEWEQ', access_token_key='43323995-hFeORuBr9aKBfGgJUlTXLgAM0xmL7QRC9Iod8sfBR', access_token_secret='oWETs4sSKEilkWetgzFpnqtnl9Dje01SN568ECx46aY')

    for x in ids:
        try:
            s = api.GetStatus(x)
            print s.AsJsonString()
        except twitter.TwitterError:
            print "No tweet found with id " + str(x)
            pass
        except urllib2.URLError:
            print "Network not reachable, waiting 5 min..."
            time.sleep(300)
            continue
            

        # Save user to DB
        # print s.AsJsonString()
        twd = json.loads(s.AsJsonString())
        # print twd
        # print twd["user"]["created_at"]

        # If a tweet is a RT, we are interested only in the original one!

        if "retweeted_status" in twd:
            twd = twd["retweeted_status"]

        # Skip abnormal, empty tweets
        if "user" not in twd:
            continue

        dates = twd["user"]["created_at"].split(" ")
        d = dates[0] + " " + dates[1] + " " + dates[2] + " " + dates[3] + " " + dates[5]
        user_ca = time.strptime(d, "%a %b %d %H:%M:%S %Y")
        user_ca_db = str(user_ca[0]) + "-" + str(user_ca[1]) + "-" + str(user_ca[2]) + " " + str(user_ca[3]) + ":" + str(user_ca[4]) + ":" + str(user_ca[5])

        try:
            if twd["user"]["geo_enabled"]:
                geo_enabled = 1 
            else:
                geo_enabled = 0
        except KeyError:
            geo_enabled = "NULL"

        try:
            time_zone = twd["user"]["time_zone"]
        except KeyError:
            time_zone = "NULL"

        try:
            url = twd["user"]["url"]
        except KeyError:
            url = "NULL"

        try:
            description = twd["user"]["description"]
        except KeyError:
            description = "NULL"

        try:
            location = twd["user"]["location"]
        except KeyError:
            location = "NULL"

        try:
            favourites_count = twd["user"]["favourites_count"]
        except KeyError:
            favourites_count = "NULL"

        try:
            followers_count = twd["user"]["followers_count"]
        except KeyError:
            followers_count = "NULL"

        try:
            friends_count = twd["user"]["friends_count"]
        except KeyError:
            friends_count = "NULL"

        try:
            lang = twd["user"]["lang"]
        except KeyError:
            lang = "NULL"

        try:
            listed_count = twd["user"]["listed_count"]
        except KeyError:
            listed_count = "NULL"

        try:
            name = twd["user"]["name"]
        except KeyError:
            name = "NULL"

        try:
            screen_name = twd["user"]["screen_name"]
        except KeyError:
            screen_name = "NULL"

        try:
            statuses_count = twd["user"]["statuses_count"]
        except KeyError:
            statuses_count = "NULL"

        try:
            utc_offset = twd["user"]["utc_offset"]
        except KeyError:
            utc_offset = "NULL"


        if twd["user"]["protected"]:
            protected = 1
        else:
            protected = 0

        if twd["user"]["profile_background_tile"]:
            profile_background_tile = 1
        else:
            profile_background_tile = 0
    
        try:
            # c.execute("INSERT INTO users (id, description) VALUES (1,%s)", (twd["user"]["description"],))

            c.execute("INSERT INTO users (created_at, description, favourites_count, followers_count, friends_count, geo_enabled, id, lang, listed_count, location, name, profile_background_color, profile_background_tile, profile_image_url, profile_link_color, profile_sidebar_fill_color, profile_text_color, protected, screen_name, statuses_count, time_zone, url, utc_offset) VALUES ('" + user_ca_db  + "', '" + smart_str(re.escape(description)) + "'," + smart_str(favourites_count) + "," + smart_str(followers_count) + "," + smart_str(friends_count) + ", " + smart_str(geo_enabled)  + "," + smart_str(twd["user"]["id"]) + ",'" + smart_str(lang) + "'," + smart_str(listed_count) + ",'" + smart_str(re.escape(location)) + "','" + smart_str(re.escape(name)) + "','" + smart_str(twd["user"]["profile_background_color"]) + "'," + smart_str(profile_background_tile) + ",'" + smart_str(twd["user"]["profile_image_url"]) + "','" + smart_str(twd["user"]["profile_link_color"]) + "','" + smart_str(twd["user"]["profile_sidebar_fill_color"]) + "','" + smart_str(twd["user"]["profile_text_color"]) + "'," + smart_str(protected) + ",'" + smart_str(screen_name) + "'," + smart_str(statuses_count) + ",'" + smart_str(time_zone) + "','" + smart_str(url) + "'," + smart_str(utc_offset) + ")")
            print "User " + smart_str(twd["user"]["id"]) + " inserted successfully"
        except _mysql_exceptions.IntegrityError:
            print "User " + smart_str(twd["user"]["id"]) + " duplicated, no worries"
            pass


        if "retweet_count" in twd:
            rt_count = twd["retweet_count"]
        else:
            rt_count = 0

        # Identify language
        # buf = cStringIO.StringIO()
        # curl = pycurl.Curl()
        # curl.setopt(curl.USERAGENT, "firefox")
        # # print str(twd["text"])
        # curl.setopt(curl.URL, "http://translate.google.com/translate_a/t?client=t&text=" + str(removeNonAscii(twd["text"])).replace(" ", "+") + "%0A&hl=en&sl=nl&tl=en&ie=UTF-8&oe=UTF-8&multires=1&prev=enter&oc=1&it=srcd_gms.2500&ssel=4&tsel=0&sc=1")
        # curl.setopt(curl.WRITEFUNCTION, buf.write)
        # curl.perform()
        # json_str = str(buf.getvalue())
        # buf.close()
        # # print json_str

        # # json_trick = json_str.replace(",,", ",\"\",").replace(",,", ",\"\",").replace("[,", "[\"\",")
        # json_trick = json_str.replace(",,", ",").replace(",,", ",").replace(",,", ",").replace(",,", ",").replace(",,", ",").replace("[,", "[").replace(",]", "]")

        # # print json_trick
        # json_res = simplejson.loads(json_trick)

        # print json_res
        # try: tw_lang = json_res[4][0][0].encode('utf-8')
        # except IndexError: tw_lang = ""
        
        # print tw_lang

        tw_lang = ""

        dates = twd["created_at"].split(" ")
        d = dates[0] + " " + dates[1] + " " + dates[2] + " " + dates[3] + " " + dates[5]
        tweet_ca = time.strptime(d, "%a %b %d %H:%M:%S %Y")
        tweet_ca_db = str(tweet_ca[0]) + "-" + str(tweet_ca[1]) + "-" + str(tweet_ca[2]) + " " + str(tweet_ca[3]) + ":" + str(tweet_ca[4]) + ":" + str(tweet_ca[5])
        
        # Save tweet to DB

        try:
            c.execute("INSERT INTO tweets (created_at, id, source, text, tweet_lang, user, retweet_count) VALUES ('" + tweet_ca_db + "'," + smart_str(twd["id"]) + ",'" + smart_str(re.escape(twd["source"])) + "','" + smart_str(re.escape(twd["text"])) + "','" + smart_str(tw_lang) + "'," + smart_str(twd["user"]["id"]) + "," + smart_str(rt_count)  + ")")
            print "Tweet " + smart_str(twd["id"]) + " inserted successfully"
        except _mysql_exceptions.IntegrityError:
            print "Tweet " + smart_str(twd["id"]) + " duplicated, no worries"

        time.sleep(11)

if __name__ == "__main__":
    main()
