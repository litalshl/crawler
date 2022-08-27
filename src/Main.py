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
        websites.delete_many({})
        urls = set()
        seeder = Seeder()
        await asyncio.get_event_loop().create_task(seeder.main(websites, urls))
        downloader = Downloader()
        await asyncio.get_event_loop().create_task(downloader.main(websites, urls))  
        websiteDocs = websites.find({})
        for document in websiteDocs:
          print(document)      

    async def __init_database(self):
        client = MongoClient(os.getenv("MONGODB_HOST"),
                             int(os.getenv("MONGODB_PORT")))

        db = client.crawler_db
        db.websites.create_index(('url'), unique=True)
        return db


asyncio.run(Main().main())