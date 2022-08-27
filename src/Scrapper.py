import sys


class Scrapper:

    async def scrape_page(self, url: str, dom: str, websites, urls):
        # Search for the <a> element and get the href, check if it is a subpage of the original website
        for elt in dom.xpath('//a'):
            scrapped_url = elt.attrib['href']
            if url in scrapped_url:                                              
                await self.__store_page_url_in_db(url, websites, scrapped_url)                
                urls.add(scrapped_url)
                print("Scrapper added new url: ", urls)

    async def __store_page_url_in_db(self, url: str, websites, scrapped_url: str):
        websiteDoc = websites.find_one({'url': url})
        websiteDoc["urls"].append(scrapped_url)        
        try:
            websites.update_one({'url': url},
                                {"$set": {
                                    "urls": websiteDoc["urls"]
                                }})            
        except:
            print("Scrapper: website document was not updated in DB, error: ",
                  sys.exc_info()[0])
