import requests
from lxml import etree
from pymongo import MongoClient
import sys
import pandas as pd
from dotenv import load_dotenv
import os
import time
import asyncio

load_dotenv()

client = MongoClient(os.getenv("MONGODB_HOST"), int(os.getenv("MONGODB_PORT")))

db = client.crawler_db
result = db.websites.create_index(('url'), unique=True)
websites = db.websites

df = pd.read_csv(os.getenv("CSV_FILE"))  
for index, row in df.iterrows():    
    try:
        websites.update_one({'url': row["website_host"]}, 
        {"$set":{
            "numPagesToDownload" : row[" number_of_pages_to_download"],        
            "maxConcurrentConnections" : int(os.getenv("MAX_CONCURRENT_CONNECTIONS_WEBSITE")),
            "downloadedPages" : 0,
            "dom" : "",
            "urls" : [],
            "pages": []
        }}, upsert=True)
    except:
        print("Website was not added to DB, error: ", sys.exc_info()[0]) 

# Downloader and scrapper basic functionality
URL = "https://www.vortex.com/"
retries = 0
try:        
    resp = requests.get(URL)
    if resp.status_code == 200:
        # Create DOM from HTML text
        dom = etree.HTML(resp.text)
        websiteDoc = client.crawler_db.websites.find_one({'url': URL})
        websiteDoc["dom"] = dom
        # Search for the <a> element and get the href, check if is a subpage of the original website
        for elt in dom.xpath('//a'):
            if URL in elt.attrib['href']:
                print(elt.attrib['href'])
                # if url doesn't already exist in the DB, add it to the relevant website document
                page = {
                    "url" : elt.attrib['href'],
                    "dom" : ""
                }
                websiteDoc["urls"].append(page["url"])
                websiteDoc["pages"].append(page)
                try:
                    client.db.collection.update_one({'url': URL}, 
                    {"$set":{
                        "dom" : str(websiteDoc["dom"]),
                        "urls" : websiteDoc["urls"], 
                        "pages" : websiteDoc["pages"]}})
                except:
                    print("Website document was not updated in DB, error: ", sys.exc_info()[0])
except:
    if retries <= int(os.getenv("MAX_DOWNLOAD_RETRIES")):
        asyncio.sleep(int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC")))
        # scrap(URL, retries)
        retries+= 1


# Tests to perform:
# 1. Settings are read and used as expected
# 2. DB - data is saved correctly and retrieved correctlly
# 3. Logic - no duplications in pages and urls, number of retries, number of open connections, 
#  number of pages downloaded as defined, correct handel of loops
# 4. Messages - no duplications of urls, message is read only once
# 5. Seeder - csv file is read correctlly, adds correct metadata to the DB website collection and 
#  correct messages are added 