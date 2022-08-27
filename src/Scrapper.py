import sys


class Scrapper:

    async def scrape_page(self, url: str, dom: str, websites, urls):
        # Search for the <a> element and get the href, check if it is a subpage of the original website
        for elt in dom.xpath('//a'):
            if url in elt.attrib['href']:                              
                page = {
                    "url": elt.attrib['href'],
                    "dom": ""
                }
                await self.__store_page_url_in_db(url, websites, page)                
                urls.add(page["url"])
                print("Scrapper added new url: ", page["url"])

    async def __store_page_url_in_db(self, url: str, websites, page: object):
        websiteDoc = websites.find_one({'url': url})
        websiteDoc["urls"].append(page["url"])
        websiteDoc["pages"].append(page)
        try:
            websites.update_one({'url': url},
                                {"$set": {
                                    "urls": websiteDoc["urls"],
                                    "pages": websiteDoc["pages"]}})
            # print("Scrapper updated DB document with: ", websiteDoc["urls"])
        except:
            print("Scrapper: website document was not updated in DB, error: ",
                  sys.exc_info()[0])
