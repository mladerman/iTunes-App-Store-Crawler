iTunes-App-Store-Crawler
========================

Multithreaded Python 3.2 webcrawler that scrapes a large portion of the app store and outputs information for each app.

The script must be run twice to be effective:

1) Set operation to "store" in order to get the initial list of links to scrape

2) Set operation to "apps" in order to get the information for those links

After running 2), a large set of csv files will be generated. You may concatenate them using the following command:

>$cat *.csv > filename

Dependencies
========================
This is written in Python 3.2 and is not compatible with 2.6 or 2.7.

The only outside package is Beautiful Soup 4 (http://www.crummy.com/software/BeautifulSoup/), which can be installed using the commands `$pip install beautifulsoup4` or `$easy_install beautifulsoup4#`.

