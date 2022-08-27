import os
import asyncio
from pymongo import MongoClient
from dotenv import load_dotenv
from Downloader import Downloader
from Seeder import Seeder
from concurrent.futures import ProcessPoolExecutor


class Main:
    async def __main__(self):
        load_dotenv()
        db = await self.__init_database()
        websites = db.websites
        urls = set()
        seeder = Seeder()
        if __name__ == '__main__':
            concurrent_connections = int(os.getenv("MAX_OVERALL_CONNECTIONS"))
            executor = ProcessPoolExecutor(concurrent_connections)
            loop = asyncio.get_event_loop()
            loop.run_in_executor(executor, await seeder.main(websites, urls))
            downloader = Downloader()
            loop.run_in_executor(executor, await downloader.main(websites, urls))        

    async def __init_database(self):
        client = MongoClient(os.getenv("MONGODB_HOST"),
                             int(os.getenv("MONGODB_PORT")))

        db = client.crawler_db
        db.websites.create_index(('url'), unique=True)
        return db


asyncio.run(Main().__main__())
