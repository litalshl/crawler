from pymongo import MongoClient
import sys

class Scrapper:

    async def scrape_page(self, url: str, dom: str, db_client: MongoClient):
        # Search for the <a> element and get the href, check if is a subpage of the original website
        for elt in dom.xpath('//a'):
            if url in elt.attrib['href']:
                print(elt.attrib['href'])
                # if url doesn't already exist in the DB, add it to the relevant website document
                page = {
                    "url": elt.attrib['href'],
                    "dom": ""
                }
                await self.__store_page_url_in_db(url, db_client, page)


    async def __store_page_url_in_db(self, url: str, db_client: MongoClient, page: object):
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