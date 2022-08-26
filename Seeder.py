import json
import os
import sys
from typing import Collection
import pandas as pd
from kafka import KafkaProducer


class Seeder:
    async def main(self, websites: Collection):
        await self.__data_seeder(websites)
        await self.__produce_url_message()

    async def __produce_url_message(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send('my-topic', {'key': 'value'})

    async def __data_seeder(self, websites: Collection):
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
