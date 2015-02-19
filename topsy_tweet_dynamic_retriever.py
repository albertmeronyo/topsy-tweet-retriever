# Script made by MariaMontenegro
#
# An extension to "topsy-tweet-retriever.py" but with the
# ability to parse dynamic webpages with the use of selenium
# 
# Info saved in .txt file in dictionary format

import json
import simplejson
import time
import datetime
import time
from datetime import date
import re
import sys
import getopt
import uuid

##
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def on_receive(data):
	print data

def main():
	print "in main"
	q = ""
	s = ""
	e = ""
	o = ""
	
	# -2. Preprocess program arguments
	optlist, args = getopt.getopt(sys.argv[1:], "hq:s:e:o:", ["help", "query=", "stardate=", "enddate=", "order="])
	#print optlist
	#print args
	for a in optlist:
		if a[0] == "-q" or a[0] == "--query":
			q = "%23" + a[1].replace(" ", "+%23")
		if a[0] == "-s" or a[0] == "--startdate":
			s = date(int(a[1].split("-")[0]), int(a[1].split("-")[1]), int(a[1].split("-")[2]))
		if a[0] == "-e" or a[0] == "--enddate":
			e = date(int(a[1].split("-")[0]), int(a[1].split("-")[1]), int(a[1].split("-")[2]))
		if a[0] == "-o" or a[0] == "--order":
			o = a[1]
		
	parseTopsy(q,s,e)

			
def parseTopsy(q,s,e):
    # 0. Parameters -- Topsy parameters and DB connection
	basetime = 1304200800 # Base time for the date-based search: 01/05/2011 00:00
	basedate = date(2011, 5, 1)
	print q,s,"||", e
	
	entered_date = datetime.datetime.strptime(str(s), '%Y-%m-%d')
	entered_date = entered_date.date()
	
	timediff = (s - basedate).days * 24 * 3600
	
	starttime = basetime + timediff
	offset = 0 # Pagination of results (10 results per HTML GET)
	ids_set = set()
	ids = []
	hours = (e - s).days * 24 # How many hours of tweets to retrieve


    # 1. Get HTML from Topsy according to parameters, and parse HTML
	print "Retrieving ids to fetch from Topsy, please wait."

	
	curTweetFeed = {}
	curTweetFeed['startTime'] = str(s)
	curTweetFeed['endTime'] = str(e)
	curTweetFeed['game'] = q
	
	counter = 0
	#for hour in range(hours):
	hour = 0
	
	name_file = q + "_" + str(uuid.uuid4()) # + ".txt"
	name_file_1 = name_file +'_1.txt'
	name_file = name_file + '.txt'
	output = open(name_file_1, 'w')
	print range(hours) 
	while hour < (hours):
		offset = 0
		
		while offset < 50:
			topsy_url = "http://topsy.com/s?q="+q+"&type=tweet&mintime=" + str(starttime + (hour * 3600)) + "&maxtime=" + str(starttime + ((hour + 5) * 3600)) + "&offset="+ str(offset)
			offset += 10
			print offset
			try:
				driver = webdriver.Firefox()
				driver.implicitly_wait(5) # seconds
				driver.get(topsy_url)#("http://topsy.com/s?q=%23usopen&type=tweet")
				myDynamicElement = driver.find_element_by_id("results")
				tweet_result = myDynamicElement.find_elements_by_class_name("result-tweet")
				#total_results =  len(tweet_result)
				for t in range(0,len(tweet_result)):
					info_div   = tweet_result[t]
					info_class = info_div.find_elements_by_class_name("media-body")
					info       = info_class[0]
					#print info.text
					id_ = str(hour) + '_'+ str(counter)
					counter += 1
					curTweetFeed[id_] = info.text
					output.write(curTweetFeed[id_])
					output.write('\n')
					
				driver.close()
			except ValueError:
				print "error"
				#name_file = q + "_" + str(uuid.uuid4()) + ".txt"
				#json.dump(curTweetFeed, open(name_file,'w'))
			finally:
				#name_file = q + "_" + str(uuid.uuid4()) + ".txt"
				#json.dump(curTweetFeed, open(name_file,'w'))
				driver.quit()
		hour +=2
	
	print "here"
	#name_file = q + "_" + str(uuid.uuid4()) + ".txt"
	json.dump(curTweetFeed, open(name_file,'w'))
	output.close()

if __name__ == "__main__":
	main()