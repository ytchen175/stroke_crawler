# stroke_crawler

###### Library : `re`, `requests`, `lxml`, `aiohttp`, `asyncio`
###### Service : `docker`, `proxy_pool`
###### Keywords : `Asynchronous Crawler`, `Regex`

# Description

To accelerate per second requests speed, I implemented the crawler through asynchronous method.

Besides, preventing requests be banned, we need to start [Proxy_pool](https://github.com/jhao104/proxy_pool) microservice in container and allocate/change proxies, user agents dynamically. 

# Details
###### Status : `WIP`
Target URL : https://www.cns11643.gov.tw/search.jsp?ID=11 