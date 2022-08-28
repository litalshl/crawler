import sys


class Scrapper:

    async def scrape_page(self, url: str, dom: str, urls):
        # Search for the <a> element and get the href, check if it is a subpage of the original website
        if dom == None:
            return
        try:
            for element in dom.xpath('//a'):
                scrapped_url = element.attrib['href']
                if url in scrapped_url:                
                        urls.add(scrapped_url)
                        print("Scrapper handeled new url: ", scrapped_url)
        except:
            print("Scrapper error: ", sys.exc_info()[0])
