import json
import os
import asyncio
from typing import Collection
import requests
from lxml import etree
import sys
from aiokafka import AIOKafkaConsumer

from Scrapper import Scrapper


class Downloader:

    async def main(self, websites):
        self.consumer = AIOKafkaConsumer('downloader-topic',
                                    bootstrap_servers=['localhost:29092'], group_id='crawler',
                                    value_deserializer=lambda m: json.loads(m.decode('ascii')))
        await self.consumer.start()
        await self.__consume_url_message(websites)

    async def __consume_url_message(self, websites):
        try:
            async for message in self.consumer:
                print("consumed: ", message.topic, message.partition, message.offset,
                      message.key, message.value, message.timestamp)
                retries = 0
                try:
                    url = message.value
                    dom = await self.__download_page(url, retries, websites)
                    scrapper = Scrapper()
                    await scrapper.scrape_page(url, dom, websites)
                except:
                    if retries <= int(os.getenv("MAX_DOWNLOAD_RETRIES")):
                        asyncio.sleep(
                            int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC")))
                        await self.__download_page(url, retries, websites)
                        retries += 1
        except:
            print("Error while consuming message from Kafka, error: ",
                  sys.exc_info()[0])
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await self.consumer.stop()

    async def __download_page(url: str, retries: int, websites: Collection):
        resp = requests.get(url)
        if resp.status_code == 200:
            # Create DOM from HTML text
            dom = etree.HTML(resp.text)
            try:
                websites.update_one({'url': url},
                                    {"$set": {
                                        "dom": str(dom)
                                    }})
            except:
                print("Website document was not updated in DB, error: ",
                      sys.exc_info()[0])
            return dom
