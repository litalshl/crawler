import json
import os
import asyncio
from typing import Collection
import requests
from lxml import etree
import sys
from Scrapper import Scrapper


class Downloader:

    async def main(self, websites):
        await self.__consume_url_message(websites)

    async def __consume_url_message(self, websites):
        try:
            # TODO: read urls from urls queue
            urls = []
            for message in urls:
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

    async def __download_page(url: str, retries: int, websites: Collection):
        resp = requests.get(url)
        if resp.status_code == 200:
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