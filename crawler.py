import json
import os
import asyncio
import requests
import pandas as pd
from typing import Collection
from lxml import etree
from pymongo import MongoClient
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka import KafkaProducer

from Seeder import Seeder


async def main():
    load_dotenv()

    client = MongoClient(os.getenv("MONGODB_HOST"),
                         int(os.getenv("MONGODB_PORT")))

    db = client.crawler_db
    db.websites.create_index(('url'), unique=True)
    websites = db.websites

    # producer = KafkaProducer(bootstrap_servers=[
    #                          'localhost:29092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # producer.send('my-topic', {'key': 'value'})

    # consumer = KafkaConsumer('my-topic',
    #                          group_id='my-group',
    #                          bootstrap_servers=['localhost:29092'],
    #                          value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # for message in consumer:
    #     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                          message.offset, message.key,
    #                                          message.value))

    seeder = Seeder()
    await seeder.main(websites)

    # TODO: extract to a consumer class that gets the url from the message
    URL = "https://www.vortex.com/"
    retries = 0
    try:
        dom = await download_page(URL, retries, client)
        await scrape_page(URL, dom, client)
    except:
        if retries <= int(os.getenv("MAX_DOWNLOAD_RETRIES")):
            asyncio.sleep(int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC")))
            await download_page(URL, retries, client)
            retries += 1


async def data_seeder(websites: Collection):
    df = pd.read_csv(os.getenv("CSV_FILE"))
    for index, row in df.iterrows():
        try:
            websites.update_one({'url': row["website_host"]},
                                {"$set": {
                                    "numPagesToDownload": row[" number_of_pages_to_download"],
                                    "maxConcurrentConnections": int(os.getenv("MAX_CONCURRENT_CONNECTIONS_WEBSITE")),
                                    "downloadedPages": 0,
                                    "dom": "",
                                    "urls": [],
                                    "pages": []
                                }}, upsert=True)
        except:
            print("Website was not added to DB, error: ", sys.exc_info()[0])


async def download_page(url: str, retries: int, db_client: MongoClient):
    resp = requests.get(url)
    if resp.status_code == 200:
        # Create DOM from HTML text
        dom = etree.HTML(resp.text)
        try:
            db_client.db.collection.update_one({'url': url},
                                               {"$set": {
                                                   "dom": str(dom)
                                               }})
        except:
            print("Website document was not updated in DB, error: ",
                  sys.exc_info()[0])
        return dom


async def scrape_page(url: str, dom: str, db_client: MongoClient):
    # Search for the <a> element and get the href, check if is a subpage of the original website
    for elt in dom.xpath('//a'):
        if url in elt.attrib['href']:
            print(elt.attrib['href'])
            # if url doesn't already exist in the DB, add it to the relevant website document
            page = {
                "url": elt.attrib['href'],
                "dom": ""
            }
            await store_page_url_in_db(url, db_client, page)


async def store_page_url_in_db(url: str, db_client: MongoClient, page: object):
    websiteDoc = db_client.crawler_db.websites.find_one({'url': url})
    websiteDoc["urls"].append(page["url"])
    websiteDoc["pages"].append(page)
    try:
        db_client.db.collection.update_one({'url': url},
                                           {"$set": {
                                               "urls": websiteDoc["urls"],
                                               "pages": websiteDoc["pages"]}})
    except:
        print("Website document was not updated in DB, error: ",
              sys.exc_info()[0])

asyncio.run(main())


# Tests to perform:
# 1. Settings are read and used as expected
# 2. DB - data is saved correctly and retrieved correctlly
# 3. Logic - no duplications in pages and urls, number of retries, number of open connections,
#  number of pages downloaded as defined, correct handel of loops
# 4. Messages - no duplications of urls, message is read only once
# 5. Seeder - csv file is read correctlly, adds correct metadata to the DB website collection and
#  correct messages are added
