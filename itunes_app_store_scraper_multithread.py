#-------------------------------------------------------------------------------
# Name:        itunes_app_store_scraper_multithread.py
# Purpose:     To crawl the itunes app store to put together a data set for
#              individual apps
#
# Author:      Mike Laderman
#
# Created:     July 2012
# Copyright:   (c) Mike Laderman 2012
# 
#-------------------------------------------------------------------------------


#!/usr/local/bin/python3.2

import urllib.request
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import sys
import time
import csv
from random import shuffle
import threading

#--------User Set These Global Variables----------#

#Set operation to "store" in order to get the initial list of links to scrape
#Set operation to "apps" in order to get the information for those links
operation = "apps"

#Set sleep time for the number of seconds between site requests (be polite!)
sleep = 0

#Set sample to an integer (not in the quotes) for the number of apps to get info for
#out of the whole file

#Set sample to '' to crawl the whole data set at once
sample = ''

#Number of threads spawned to call the itunes store at one time. Must be an integer.
threads = 40

#Set links_file to the name of the file with the initial list of links to scrape
# Set app_info_file to the name of the file with the information for those links
links_file = 'App_Store_Links.csv'
app_info_file = 'App_Store_Info1.csv'


#--------DO NOT TOUCH!----------#
nav_site = "http://itunes.apple.com/us/genre/ios-books/id6018?mt=8"
alphabet = ['#','A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

#--------Program Functions----------#
def site_open(site):
    '''Makes connection and opens up target website. Returns a website object.'''
    try:
        #sets up request object
        req = urllib.request.Request(site)

        #adds User-Agent info to request object
        req.add_header("User-Agent","Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) \
         AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5")

        #opens up site
        website = urllib.request.urlopen(req)

        return website
    except urllib.request.URLError:
           print('Could not connect to '+ site + '!')
           pass

def soup_site(site):
    '''opens site and turns it into a format to easily parse the DOM. Returns a Soup Object'''
    return BeautifulSoup(site_open(site))

def title_get(soup):
    '''Returns App Title Text'''
    #title is the text in the <h1> tag in the "title" <div>
    return soup.find(id="title").find("h1").text

def dev_get(soup):    
    '''Returns developer name text'''
    #dev name is the text in the <h2> tag in the "title" <div>
    return soup.find(id="title").find("h2").text[3:]

def price_get(soup):
    '''Returns price in $'''
    #price is the text in the first <li> in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find("li").text

def category_get(soup):
    '''Returns Category Name'''
    #category is the text in the <a> tag in the <li> tag with class="genre"
    #in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find("li","genre").find("a").text

def size_get(soup):
    '''Returns size of file in MB'''
    #size is the text in the 5th <li> tag in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find_all("li")[4].text[6:]

def seller_get(soup):
    '''Returns Seller Name'''
    #seller is the text in the 6th <li> tag in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find_all("li")[6].text[8:]

def rating_get(soup):
    '''Returns tuple (# of Stars, # of Ratings)'''

    #rating is located in the "aria-label" tag in the <div class="rating">
    #in the <div class="customer-ratings>
    tag = soup.find("div","customer-ratings").find("div","rating")

    #splits array of stars and rating from the "aria-label" tag
    stars,rating = tag['aria-label'].split(',')

    #return a tuple of stars and rating without whitespace
    return (stars.strip(),rating.strip())

def app_info(soup,site):
    '''Aggregates info from single app and prints it into a tuple.\n
    Returns (title, dev, price, category, size, seller, stars, rating, url)'''
    try:

        #Makes a call to each subfunction and assigns value to local var
        title = title_get(soup)
        developer = dev_get(soup)
        price = price_get(soup)
        category = category_get(soup)
        size = size_get(soup)
        seller = seller_get(soup)
        stars,rating = rating_get(soup)

        #returns tuple of the app information
        return (title,developer,price,category,size,seller,stars,rating,site)
    except:
        print("I Dunno, There Was Some Error!")
        pass
    

def genre_link_list(site):
    '''Generator function that 
    outputs each genre in the app store when called
    to be used when getting general App Store Links.
    Yields a genre url.'''

    #open site and makes navigatable DOM 
    soup = soup_site(site)

    #creates array of genre links
    table = soup.find(id="genre-nav").find_all("a")


    for link in table:
        #returns next link when called
        yield link.get('href')

def app_link_list(site):
    '''Generator function that 
    outputs a single specific app link in the app store when called
    to be used when getting general App Store Links.
    Yields an app url.'''

    #open site and makes navigatable DOM 
    soup = soup_site(site)

    #makes table of app links on a specific page
    table = soup.find(id="selectedcontent").find_all("a")
    for link in table:
        #outputs tuple with the link that will be written to a csv file
        yield (link.get('href'),)

def general_app_store_crawl(file_name,sleep_time=float):
    '''Creates list of iTunes app store links.
    Grabs the first page of links for the first letter of the alphabet
    for every genre and writes it to a csv file.

    Inputs:
    1) file_name is the name of the output file_name
    2) sleep_time is a float value that indicates pause between site requests.

    Output: A csv file that lists most US iTunes store apps'''

    
    #creates and/or appends csv file
    f = open(file_name,'a',newline = '')
    writer = csv.writer(f,dialect = 'excel',quoting=csv.QUOTE_NONNUMERIC)

    #grabs everything from the database
    for i,link in enumerate(genre_link_list(nav_site)):

        print('Scraping genre '+ str(i) +'.')

        #loops through the alphabet to generate relevant sites
        #and write to csv
        for letter in alphabet:

            #reconstructs URL
            new_site = link + "&" + letter
            print('Scraping from ' + new_site + '.')

            #pause for politeness
            time.sleep(sleep_time)

            #iteratively calls the app_link_list() function
            #to write individual app links into the csv file
            for app in app_link_list(new_site):

                #writes single link into a csv file line
                writer.writerow(app)
        
    #closes file (duh)
    f.close()
    print('Completed Scrapping')
    return

def read_in(file):
    '''reads in app store links from the app links file.
    Returns array.'''
    f = open(file,'r')
    data = f.readlines()

    #return array of links
    return data


def split_data(data,splits):
    '''Returns an array of arrays [[data1],[data2],...]
    based on the number of threads the user designates.
    This is used for multithreading purposes'''
    n = round(len(data)/splits)
    print(len(data))
    new_data = []
    for i in range(0, splits):
        j = data[(i-1)*n:i*n]
        new_data.append(j)
    return new_data

def app_info_crawl(source,output,sleep_time=float,sample_size=None,num_threads=1):
    '''Takes in Source URLs and outputs App Info

    Inputs:
    1) source is the name of the input file with the list of urls to crawl
    2) output is the name of the output file_name with full app information
    3) sleep_time is a float value that indicates pause between site requests.
    4) num_threads is the number of threads that the script spawns to speed up process.

        
    Returns X number of csv files with app info, where X is equal to the number
    of threads used to grab app info. You can merge them all together in the command line
    using the command "cat *.csv > output_file"'''

    #reads in list of apps
    data = read_in(source)

    #randomizes data order to get a random sample
    shuffle(data)

    #breaks up data into an array of smaller data arrays
    #for multithreading purposes
    data = split_data(data,num_threads)

    #checks if sample_size is designated
    if sample_size != '':

        #checks if sample size is an integer and smaller than the size of the dataset
        if isinstance(sample_size,int) and sample_size < len(data):

            #makes data the size of the sample size
            data = data[0:sample_size]
        else:
            raise ValueError('Your sample_size needs to be an integer and smaller than the size of the data set.')

    #loops through data to create a new thread that will loop through the store and find 
    #a link, crawl the site, and enter the data
    #into the output csv
    for i,link_list in enumerate(data):

        #opens a csv file formatted as Links(i).csv according to the number of threads
        f = open('Links'+str(i)+'.csv','a',newline = '')
        writer = csv.writer(f,dialect = 'excel',quoting=csv.QUOTE_NONNUMERIC)

        #spawns thread and starts scrapping
        t = threading.Thread(target = app_crawl_main_loop,args = (link_list,writer) )
        t.start()
    print('Completed Spawning Threads')
    return

def app_crawl_main_loop(data,writer):
    '''Called by a thread in app_info_crawl(). Loops through
    a sub-data array and writes output to a sub-csv file.'''
    for i,link in enumerate(data):
        
        print("Scrapping #"+str(i)+".")
        try:
            info = app_info(soup_site(link.strip('"')),link.strip('"'))
            #opens the site, parses out the data, and writes it into the csv
            writer.writerow(info)
        except:
            #continues loop of error is found and skips entry
            print("Could not write to csv file for some reason =0")
            continue
    print('Completed Scrapping Data')
    return

def main():
    '''Main function that runs either the general app store crawler
    or the individual app crawler, depending on what "operation" is set to
    at the head of the script.'''
    
    #runs the main store crawl and prints out total time spent
    if operation == 'store':
        start_time = time.time()
        general_app_store_crawl(links_file,sleep)
        print(time.time()-start_time)

    #runs the app info crawl and prints out total time spent
    elif operation == 'apps':
        start_time = time.time()
        app_info_crawl(links_file,app_info_file,sleep,sample,threads)
        print(time.time()-start_time)
    else:
        print('You need to set "operation" to "store" or "apps"!')       
    return

if __name__ == '__main__':
    main()

    
        
    
    