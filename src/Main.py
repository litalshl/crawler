import os
import asyncio
from pymongo import MongoClient
from dotenv import load_dotenv
from Downloader import Downloader

from Seeder import Seeder


class Main:
    async def main(self):
        load_dotenv()
        db = await self.__init_database()
        websites = db.websites

        seeder = Seeder()
        await seeder.main(websites)
        downloader = Downloader()
        await downloader.main(websites)

    async def __init_database(self):
        client = MongoClient(os.getenv("MONGODB_HOST"),
                             int(os.getenv("MONGODB_PORT")))

        db = client.crawler_db
        db.websites.create_index(('url'), unique=True)
        return db


asyncio.run(Main().main())


# Tests to perform:
# 1. Settings are read and used as expected
# 2. DB - data is saved correctly and retrieved correctlly
# 3. Logic - no duplications in pages and urls, number of retries, number of open connections,
#  number of pages downloaded as defined, correct handel of loops
# 4. Messages - no duplications of urls, message is read only once
# 5. Seeder - csv file is read correctlly, adds correct metadata to the DB website collection and
#  correct messages are added
