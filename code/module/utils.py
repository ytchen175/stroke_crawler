import os
import csv
import json
import time
import random
import asyncio
import aiohttp
import requests
import logging

from fake_useragent import UserAgent
from .metadata import CONCURRENT_LIMIT

LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'
logging.basicConfig(level=logging.ERROR, filename='crawler.log', filemode='a', format=LOGGING_FORMAT)

user_agent = UserAgent()

""" Logging """

def logging_and_print(string):

    logging.critical(string)
    print(string)

""" Files """

def create_file(output_file):

    if os.path.exists(output_file) == False:
        open(output_file, "w").close
        logging_and_print(f'[Crawler] {output_file} created.')

        return create_file(output_file)

    else:

        with open(output_file, 'r+', encoding='UTF-8') as f:
            line = f.readline()

        if "字" in line and "筆畫" in line:
            logging_and_print(f'[Crawler] {output_file} existed.')

        else:
            with open(output_file, 'a', newline='', encoding='UTF-8') as f:
                writer = csv.writer(f)
                writer.writerow(['字', '筆畫']) # header
                logging_and_print(f'[Crawler] {output_file} header added.')

""" Network """

def get_proxy(proxy_pool_url, option='get'):
    
    proxy_pool = f'{proxy_pool_url}/{option}/'

    r = getPage(proxy_pool)
    response = json.loads(r)
    
    if option == 'get':

        return response['proxy']
        
    elif option == 'all':
        available_proxies = [data['proxy'] for data in response]

        return available_proxies

    elif option == 'count':
        available_proxies_count = response['count']['total']

        return available_proxies_count

def getPage(url, max_retries=5, proxy=None):

    times = 0

    while times < max_retries:

        try:
            session = requests.session()

            if proxy:
                proxies = {'http' : f'http://{proxy}'}
                session.proxies.update(proxies)

            res = session.get(
                url,
                headers={'user-agent': user_agent.random},
                timeout=(5.05, 27)
                ) # connect & read timeout
                
            session.cookies.clear()
            htmltext = res.text
            
            return htmltext

        except requests.exceptions.RequestException:
            times += 1

async def getPage_async(url, semaphore, max_retries=5, proxy=None):

    times = 0
    proxy = f'http://{proxy}'

    while times < max_retries:

        try:
            async with semaphore:
                async with aiohttp.ClientSession(headers={'user-agent': user_agent.random}, timeout=aiohttp.ClientTimeout(total=7)) as session:
                    async with session.get(url, proxy=proxy) as res:
                        time.sleep(1)
                        return await res.text()

        except asyncio.exceptions.TimeoutError:

            times += 1

            delay = [1, 3, 5, 7]
            time.sleep(random.choice(delay))