import json
import os
import asyncio
import requests
from lxml import etree
from pymongo import MongoClient
import sys
from aiokafka import AIOKafkaConsumer

from Scrapper import Scrapper


class Downloader:

    async def main(self, client):
        consumer = AIOKafkaConsumer('downloader-topic',
                                 bootstrap_servers=['localhost:29092'],
                                 value_deserializer=lambda m: json.loads(m.decode('ascii')))
        await self.__consume_url_message(consumer, client)

    async def __consume_url_message(self, consumer, client):
        async for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key,
                                                 message.value))

            retries = 0
            try:
                url = message.value
                dom = await self.__download_page(url, retries, client)
                scrapper = Scrapper()
                await scrapper.scrape_page(url, dom, client)
            except:
                if retries <= int(os.getenv("MAX_DOWNLOAD_RETRIES")):
                    asyncio.sleep(int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC")))
                    await self.__download_page(url, retries, client)
                    retries += 1

    async def __download_page(url: str, retries: int, db_client: MongoClient):
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
