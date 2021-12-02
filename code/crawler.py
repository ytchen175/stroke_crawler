import re
import csv
import time
import random
import asyncio
import aiohttp 
import pandas as pd

from module.utils import *
from module.functions import *
from module.metadata import BASE_URL, PROXY_POOL_URL, REFRESH_FREQUENCY, CONCURRENT_LIMIT, OUTPUT_FILENAME, FAILED_OUTPUT_FILENAME

def main():
    
    tmp = {}

    request_times = 0
    
    htmltext = getPage(BASE_URL)

    """ Find out how many difference characters in each stroke number """
    stroke_and_nums_list = get_stroke_and_nums_list(htmltext)
    stroke_and_nums_df = pd.DataFrame(stroke_and_nums_list, columns = ['筆劃', '字數'])
    stroke_and_nums_df['字數'] = stroke_and_nums_df['字數'].astype(int)
    # stroke_and_nums_df.to_csv('stroke_and_nums.csv', index=0)

    waiting_queue = stroke_and_nums_df

    queue = list(map(lambda x : re.search("[0-9]+", x).group(), waiting_queue['筆劃']))

    for stroke in queue:
        tmp.update(get_last_page_num(BASE_URL, stroke))
        
    logging_and_print(f'[Crawler] Last page num of each character scrapped.')

    stroke_and_last_page_num_df = pd.Series(tmp).rename_axis('stroke').to_frame('last_page_num').reset_index()

    """" Define url list to be traversed """
    urls_list = []

    stroke_lst = list(map(lambda x : int(x), stroke_and_last_page_num_df['stroke'].tolist()))
    stroke_last_page_num = list(map(lambda x : int(x), stroke_and_last_page_num_df['last_page_num'].tolist()))

    for stroke, last_page_num in zip(stroke_lst, stroke_last_page_num):
        for page_num in list(map(lambda x : x+1, range(last_page_num))):
            urls_list.append(combine_urls(BASE_URL, stroke, page_num))

    """" Start Crawling """
    create_file(OUTPUT_FILENAME)

    for url in urls_list:

        try:
            delay = [1, 3, 5, 7]
            time.sleep(random.choice(delay))
            
            with open(OUTPUT_FILENAME, 'a', newline='', encoding='UTF-8') as f:

                writer = csv.writer(f)

                if request_times % REFRESH_FREQUENCY == 0:
                    
                    origin_proxy = proxy

                    proxy = get_proxy(PROXY_POOL_URL) 
                    logging_and_print(f"[Crawler] Changed {origin_proxy} to {proxy}.") 
                
                writer.writerows(get_words_by_url(url, proxy=proxy))

                request_times += 1
                logging_and_print(f"[Crawler] {url} scrapped, requests time : {request_times}.")    
                
        except:
            with open(FAILED_OUTPUT_FILENAME, 'a+') as f:
                f.write(f'{url}\n')

            logging_and_print(f"[Crawler] Can't get {url}.")

if __name__ == "__main__":
    main()