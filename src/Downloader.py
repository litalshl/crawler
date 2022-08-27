import json
import os
import asyncio
import requests
from lxml import etree
import sys
import urllib
from Scrapper import Scrapper


class Downloader:

    async def main(self, websites, urls):
        self.urls = urls
        self.download_retries = int(os.getenv("MAX_DOWNLOAD_RETRIES"))
        self.retries_interval = int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC"))
        await self.__consume_url_message(websites)

    async def __consume_url_message(self, websites):
        try:
            retries = 0
            while self.urls:
                try:
                    url = self.urls.pop()
                    dom = await asyncio.get_event_loop().create_task(self.__download_page(url, retries, websites))
                    # print("Downloader updated DB with page content: ", etree.tostring(dom, encoding='unicode', pretty_print=True))  # for debug
                    scrapper = Scrapper()
                    await asyncio.get_event_loop().create_task(scrapper.scrape_page(url, dom, websites, self.urls))
                except:
                    if retries <= self.download_retries:
                        await asyncio.sleep(self.retries_interval)
                        await self.__download_page(url, retries, websites)
                        retries += 1
        except:
            print("Error while reading page, error: ",
                  sys.exc_info()[0])

    async def __download_page(self, url: str, retries: int, websites):
        resp = requests.get(url)
        if resp.status_code == 200:
            dom = etree.HTML(resp.text)
            website_url = urllib.parse.urljoin(url, '/')
            try:
                if website_url == url:
                    self.__update_website_dom(websites, url, dom)
                else:
                    await self.__update_subpage_dom(websites, url, dom, website_url)
                print("Downloader updated DB with page content for page: ", url)
            except:
                print("Downloader: website document was not updated in DB, error: ",
                      sys.exc_info()[0])
            return dom

    def __update_website_dom(self, websites, url, dom):
        websites.update_one({'url': url},
                            {"$set": {
                                "dom": str(dom)
                            }})

    async def __update_subpage_dom(self, websites, url, dom, website_url):
        website_doc = websites.find_one({'url': website_url})
        pages = website_doc['pages']
        pages.append({
            "url": url,
            "dom": str(dom)
        })
        websites.update_one({'url': website_url},
                    {"$set": {
                        "pages": pages
                    }}) 
