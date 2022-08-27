import sys


class Scrapper:

    async def scrape_page(self, url: str, dom: str, websites, urls):
        # Search for the <a> element and get the href, check if it is a subpage of the original website
        for elt in dom.xpath('//a'):
            scrapped_url = elt.attrib['href']
            if url in scrapped_url:                                                           
                urls.add(scrapped_url)
                print("Scrapper handeled new url: ", scrapped_url)