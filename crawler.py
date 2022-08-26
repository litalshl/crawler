# Assumes a local MongoDB is installed: https://www.mongodb.com/try/download/community?tck=docs_server
# pip install requests lxml pymongo
import requests
from lxml import etree

# Downloader and scrapper basic functionality
URL = "https://www.vortex.com/"
resp = requests.get(URL)
if resp.status_code == 200:
    # Create DOM from HTML text
    dom = etree.HTML(resp.text)
    # Search for the <a> element and get the href, check if is a subpage of the original website
    for elt in dom.xpath('//a'):
        if URL in elt.attrib['href']:
            print(elt.attrib['href'])

