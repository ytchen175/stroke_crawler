import re

import asyncio
import argparse
import pandas as pd

from module.utils import *
from module.functions import *
from module.metadata import BASE_URL, PROXY_POOL_URL, OUTPUT_FILENAME, FAILED_OUTPUT_FILENAME

async def main(argument):
    
    tmp = {}
    
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
            
    if argument.SN and argument.PAGE:

        urls_list = resume_progress(urls_list, BASE_URL, argument.SN, argument.PAGE)
        print(f"[Crawler] Resume progress from SN={argument.SN} and PAGE={argument.PAGE}, {len(urls_list)} urls lefting...")

    """" Start Crawling """
    create_file(OUTPUT_FILENAME)

    finished = set()
    unfinished = set(urls_list)

    while unfinished:

        available_proxies = get_proxy(PROXY_POOL_URL, option='all', type='https')

        batch_size = len(available_proxies) 

        waiting = set(random.sample(unfinished, batch_size))

        logging_and_print(f'[Crawler] Now handling URLs : {waiting}.')

        await get_words_by_url_async(waiting, available_proxies) 
        
        finished.update(waiting)

        unfinished -= waiting

        logging_and_print(f'[Crawler] Finished : {finished}, {len(unfinished)} URLs remained.')

    else:
        logging_and_print(f'[Crawler] All done !')

def _parse_args():
    """Parses command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Initial or Resume Progress. If resume, then specify SN and page.")

    parser.add_argument(
        '--SN',
        help='Resume progress from SN = ?.',
        dest='SN',
        default=None
    )

    parser.add_argument(
        '--PAGE',
        help='Resume progress from PAGE = ?.',
        dest='PAGE',
        default=None
    )
    
    args, unknown = parser.parse_known_args()
    
    return args

if __name__ == "__main__":
    argument = _parse_args()
    asyncio.run(main(argument))