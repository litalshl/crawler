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
        self.dom = None
        self.urls = urls
        self.download_retries = int(os.getenv("MAX_DOWNLOAD_RETRIES"))
        self.retries_interval = int(os.getenv("DOWNLOAD_RETRIES_INTERVAL_SEC"))
        await self.__consume_url_message(websites)

    async def __consume_url_message(self, websites):
        try:
            while self.urls:
                    url = self.urls.pop()
                    website_url = urllib.parse.urljoin(url, '/')
                    website_doc = websites.find_one({"url": website_url})
                    if website_doc:
                        if website_doc["downloadedPages"] >= website_doc["numPagesToDownload"]:
                            continue
                        if url in website_doc["urls"]:
                            continue
                    await asyncio.get_event_loop().create_task(self.__download_page(url, websites))                    
                    scrapper = Scrapper()
                    if self.dom != None:
                        await asyncio.get_event_loop().create_task(scrapper.scrape_page(url, self.dom, self.urls))
        except:
            print("Error while reading page, error: ",
                  sys.exc_info()[0])

    async def __download_page(self, url: str, websites):        
        retries = 0
        while retries <= self.download_retries:
            try:
                retries += 1
                resp = requests.get(url)
            except:
                await asyncio.sleep(self.retries_interval)
                continue       
            if resp.status_code == 200:
                self.dom = etree.HTML(resp.text)
                website_url = urllib.parse.urljoin(url, '/')
                try:
                    if website_url == url:
                        self.__update_website_dom(websites, url)
                    else:
                        await self.__update_subpage_dom(websites, url, website_url)
                    print("Downloader updated DB with page content for page: ", url)
                except:
                    print("Downloader: website document was not updated in DB, error: ",
                        sys.exc_info()[0])            

    def __update_website_dom(self, websites, url):
        websites.update_one({'url': url},
                            {"$set": {
                                "dom": str(self.dom)
                            }})

    async def __update_subpage_dom(self, websites, url, website_url):
        website_doc = websites.find_one({'url': website_url})
        urls = website_doc["urls"]
        urls.append(url)
        pages = website_doc['pages']
        pages.append({
            "url": url,
            "dom": str(self.dom)
        })
        downloaded_pages = website_doc['downloadedPages']
        downloaded_pages+= 1
        websites.update_one({'url': website_url},
                    {"$set": {
                        "pages": pages,
                        "downloadedPages": downloaded_pages,
                        "urls": urls
                    }}) 
