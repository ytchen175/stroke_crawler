import re
import csv
import asyncio
import aiohttp 

from lxml import etree
from .utils import getPage, getPage_async, logging_and_print
from .metadata import OUTPUT_FILENAME, FAILED_OUTPUT_FILENAME, CONCURRENT_LIMIT

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

def combine_urls(base_url, stroke, page_num):

    stroke_query = f'&SN={stroke}'
    page_query = f'&PAGE={page_num}'

    target_url = base_url + stroke_query + page_query
    
    return target_url

def get_words_by_url(target_url, proxy=None):

    stroke = re.search('(&SN=)([0-9]+)', target_url).group(2)

    htmltext = getPage(target_url, proxy=proxy)

    xdoc = etree.HTML(htmltext)

    if "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '筆畫查詢':
        words = list(xdoc.xpath("//div[@class='wordList']/span/a/img/@alt"))

    elif "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '字形資訊':
        words = list(xdoc.xpath("//div[@class='col2 Lt Ft'][2]//div[2]/img/@alt"))

    dicts = list(zip(words, [stroke] * len(words)))
    
    return iter(dicts)
    
async def get_words_by_url_async(urls_list, available_proxies, concurrent_limit=CONCURRENT_LIMIT, output_file=OUTPUT_FILENAME, failed_output=FAILED_OUTPUT_FILENAME):
    
    batch_size = len(available_proxies)
    
    if batch_size > CONCURRENT_LIMIT:
        concurrent_limit = batch_size

    semaphore = asyncio.Semaphore(concurrent_limit)

    htmltext_lst = await asyncio.gather(*[getPage_async(target_url, semaphore=semaphore, proxy=proxy) for target_url, proxy in zip(urls_list, available_proxies)])

    try:
        for htmltext, target_url in zip(htmltext_lst, urls_list):

            stroke = re.search('(&SN=)([0-9]+)', target_url).group(2)

            xdoc = etree.HTML(htmltext)

            if "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '筆畫查詢':
                words = list(xdoc.xpath("//div[@class='wordList']/span/a/img/@alt"))

            elif "".join(xdoc.xpath("//table[@class='frame']//td[@class='pageName']/text()")) == '字形資訊':
                words = list(xdoc.xpath("//div[@class='col2 Lt Ft'][2]//div[2]/img/@alt"))

            dicts = list(zip(words, [stroke] * len(words)))

            with open(output_file, 'a', newline='', encoding='UTF-8') as f:
                writer = csv.writer(f)
                writer.writerows(iter(dicts))

                logging_and_print(f"[Crawler] {target_url} scrapped.")  

    except:
        with open(failed_output, 'a+') as f:
            f.write(f'{target_url}\n')

        logging_and_print(f"[Crawler] Can't get {target_url}.")