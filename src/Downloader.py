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
                    url = self.urls.pop()
                    website_url = urllib.parse.urljoin(url, '/')
                    website_doc = websites.find_one({"url": website_url})
                    if website_doc and website_doc["downloadedPages"] >= website_doc["numPagesToDownload"]:
                        continue
                    dom = await asyncio.get_event_loop().create_task(self.__download_page(url, retries, websites))                    
                    scrapper = Scrapper()
                    await asyncio.get_event_loop().create_task(scrapper.scrape_page(url, dom, websites, self.urls))
        except:
            print("Error while reading page, error: ",
                  sys.exc_info()[0])

    async def __download_page(self, url: str, retries: int, websites):
        try:
            retries += 1
            resp = requests.get(url)
        except:
            if retries <= self.download_retries:
                await asyncio.sleep(self.retries_interval)
                await self.__download_page(url, retries, websites)   
                return None          
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
        downloaded_pages = website_doc['downloadedPages']
        downloaded_pages+= 1
        websites.update_one({'url': website_url},
                    {"$set": {
                        "pages": pages,
                        "downloadedPages": downloaded_pages
                    }}) 
