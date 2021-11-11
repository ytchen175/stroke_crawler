import re
import csv
import time
import random
import pandas as pd
import logging

from module.metadata import url, output_file
from module.functions import *

LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'
logging.basicConfig(level=logging.ERROR, filename='crawler.log', filemode='a', format=LOGGING_FORMAT)

def logging_and_print(string):
    logging.critical(string)
    print(string)

def main():
    
    htmltext = getPage(url)

    stroke_and_nums_list = get_stroke_and_nums_list(htmltext)
    stroke_and_nums_df = pd.DataFrame(stroke_and_nums_list, columns = ['筆劃', '字數'])
    stroke_and_nums_df['字數'] = stroke_and_nums_df['字數'].astype(int)
    # stroke_and_nums_df.to_csv('stroke_and_nums.csv', index=0)

    # 要爬的
    waiting_queue = stroke_and_nums_df

    tmp = {}

    queue = list(map(lambda x : re.search("[0-9]+", x).group(), waiting_queue['筆劃']))

    for stroke in queue:
        tmp.update(get_last_page_num(url, stroke))
        
    logging_and_print(f'[Crawler] Last page num of each character scrapped.')

    stroke_and_last_page_num_df = pd.Series(tmp).rename_axis('stroke').to_frame('last_page_num').reset_index()

    # word_stroke_dict = {}

    stroke_lst = list(map(lambda x : int(x), stroke_and_last_page_num_df['stroke'].tolist()))
    stroke_last_page_num = list(map(lambda x : int(x), stroke_and_last_page_num_df['last_page_num'].tolist()))
    # stroke_lst = [4, 5]
    # stroke_last_page_num = [12, 19]

    # Instantiate
    create_file(output_file) 

    for stroke, last_page_num in zip(stroke_lst, stroke_last_page_num):
        for page_num in list(map(lambda x : x+1, range(last_page_num))):

            try:
                delay = [1, 3, 5, 30]
                time.sleep(random.choice(delay))
                
                with open(output_file, 'a', newline='', encoding='UTF-8') as f:

                    writer = csv.writer(f)

                    writer.writerows(get_words_by_stroke_and_page_num(url, stroke, page_num))
                    logging_and_print(f"[Crawler] Got stroke:{stroke} & page_num {page_num}")
                    
                    f.close()
            except:
                logging_and_print(f"[Crawler] Error Occured at stroke:{stroke} & page_num {page_num}")

if __name__ == "__main__":
    main()