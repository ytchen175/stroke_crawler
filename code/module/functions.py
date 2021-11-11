import os
import csv
import requests
import pandas as pd

from lxml import etree
from fake_useragent import UserAgent

user_agent = UserAgent()

def getPage(url, max_retries=5):

    times = 0

    while times < max_retries:

        try:
            session = requests.session()
            res = session.get(url, headers={'user-agent': user_agent.random}, timeout=(5.05, 27)) # connect & read timeout
            session.cookies.clear()
            htmltext = res.text
            
            return htmltext

        except requests.exceptions.RequestException:
            times += 1

# 獲取全筆劃 & 字數
def get_stroke_and_nums_list(htmltext):

    xdoc = etree.HTML(htmltext)

    stroke_list = list(xdoc.xpath("//td/div[@class='float2 part8p']/a/text()"))
    number_list = list(map(lambda x : x.replace("(", "").replace(")", ""), list(xdoc.xpath("//td/div[@class='float2 part8p']/a/span/text()"))))

    stroke_and_nums_list = list(zip(stroke_list, number_list))
    
    return stroke_and_nums_list

# 取得各頁面最後一頁的頁數
def get_last_page_num(base_url, stroke):

    stroke_query = f'&SN={stroke}'

    target_url = base_url + stroke_query

    htmltext = getPage(target_url)

    xdoc = etree.HTML(htmltext)

    if "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '筆畫查詢':
        last_page_num = "".join(xdoc.xpath("//div[@class='pager']/span/text()")).replace('[', '').replace(']', '').split('/')[1]

    elif "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '字形資訊':
        last_page_num = '1'
    
    return {stroke : last_page_num}

def get_words_by_stroke_and_page_num(base_url, stroke, page_num):

    stroke_query = f'&SN={stroke}'
    page_query = f'&PAGE={page_num}'

    target_url = base_url + stroke_query + page_query

    htmltext = getPage(target_url)

    xdoc = etree.HTML(htmltext)

    if "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '筆畫查詢':
        words = list(xdoc.xpath("//div[@class='wordList']/span/a/img/@alt"))

    elif "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '字形資訊':
        words = list(xdoc.xpath("//div[@class='col2 Lt Ft'][2]//div[2]/img/@alt"))

    # dicts = dict(zip(words, cycle([stroke])))
    dicts = list(zip(words, [stroke] * len(words)))
    
    return iter(dicts)

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