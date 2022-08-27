# crawler 

This crawler gets a CSV file as an input with the below structructure:

```
website_host, number_of_pages_to_download

http://itcorp.com/, 4

https://www.vortex.com/, 7
```

For each website host in the CSV file, the crawler will save it's HTML content and scrap all the urls from the page.

For all referred pages that are on the same website host, the crawler will do the same, save their content and scrap the urls for more unreached pages.

This diagram describes the application components and interactions:

![ScreenShot](CrawlerDesign.jpg)

The crawler saves all data in MongoDB. Currently in a single websites collection, with the below document schema:
```
    url : string <index>
    numPagesToDownload : int   
    maxConcurrentConnections : int
    downloadedPages : int
    dom : string
    urls : string[]
    pages: [
        {
            url : string
            dom : string
        }
    ]
```
- The application requires MongoDB. Connection details can be defined in the .env file as MONGODB_HOST and MONGODB_PORT. Currently supports no authentication connection only.
For quick localhost install, you can refer to this website: 
https://www.mongodb.com/try/download/community?tck=docs_server

- The application uses the following libraries:
```
pip install requests lxml pymongo pandas python-dotenv aiokafka
```
you can also use the requirements file
```
pip install -r requirements.txt
```

- To use different csv file, set in the .env file the parameter
``` 
CSV_FILE 
```

- Additional parameters that can be changed in the .env file:
```
MAX_CONCURRENT_CONNECTIONS_WEBSITE - max concurrent connections per website - default is 2
MAX_OVERALL_CONNECTIONS - max overall connections defined for the whole application - default is 10
MAX_DOWNLOAD_RETRIES - max number of retries when page download failed - default is 3
DOWNLOAD_RETRIES_INTERVAL_SEC - time between download retries in seconds - default is 5
```
- For scalable use, it is recommended to deploy each component on a seperate container and change the urls queue to Kafka topic.