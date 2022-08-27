import os
import sys
import pandas as pd


class Seeder:

    async def main(self, websites, urls):
        self.urls = urls
        await self.__data_seeder(websites)

    async def __produce_url_message(self, url: str):
        try:            
            self.urls.add(url)
            print("Seeder added new url: ", url)
        except:
            print("Exception while producing url message, error: ",
                  sys.exc_info()[0])

    async def __data_seeder(self, websites):
        dataframe = pd.read_csv(os.getenv("CSV_FILE"))
        for index, row in dataframe.iterrows():
            try:
                websites.update_one({'url': row["website_host"]},
                                    {"$set": {
                                        "numPagesToDownload": row[" number_of_pages_to_download"],
                                        "maxConcurrentConnections": int(os.getenv("MAX_CONCURRENT_CONNECTIONS_WEBSITE")),
                                        "downloadedPages": 0,
                                        "dom": "",
                                        "urls": [],
                                        "pages": []
                                    }}, upsert=True)
            except:
                print("Website was not added to DB, error: ",
                      sys.exc_info()[0])

            await self.__produce_url_message(row["website_host"])