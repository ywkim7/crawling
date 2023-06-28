import requests
import pymongo
import logging
import traceback
import multiprocessing
import parmap
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from tqdm import tqdm


WEBDRIVER_PATH = "C:/Users/user/Documents/kyw/크롤링/chromedriver_win32/chromedriver.exe"
WEBDRIVER_OPTIONS = Options()
WEBDRIVER_OPTIONS.add_argument('--headless')

svc = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=svc, options=WEBDRIVER_OPTIONS)


def ex_tag(date):
    tag_list = []

    for page in range(1, 31):
        url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=052&date={date}&page={page}"
        driver.get(url)
        driver.implicitly_wait(2)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        for i in range(10):
            tmp_str = f"#main_content > div.list_body.newsflash_body > ul.type06_headline > li:nth-child({i + 1}) > dl > dt > a"
            tmp_ex_str = f"#main_content > div.list_body.newsflash_body > ul.type06_headline > li:nth-child({i + 1}) > dl > dt:nth-child(2) > a"
            try:
                tag = soup.select(tmp_str)[0].attrs['href']
            except IndexError:
                break
            except:
                tag = soup.select(tmp_ex_str)[0].attrs['href']
                logging.error(traceback.format_exc())
            tag_list.append(tag)

        for i in range(10):
            tmp_str = f"#main_content > div.list_body.newsflash_body > ul.type06 > li:nth-child({i + 1}) > dl > dt > a"
            tmp_ex_str = f"#main_content > div.list_body.newsflash_body > ul.type06 > li:nth-child({i + 1}) > dl > dt:nth-child(2) > a"

            try:
                tag = soup.select(tmp_str)[0].attrs['href']
            except IndexError:
                break
            except:
                tag = soup.select(tmp_ex_str)[0].attrs['href']
                logging.error(traceback.format_exc())
            tag_list.append(tag)

    return tag_list

def re_tag():
    num_workers = multiprocessing.cpu_count()

    re_list = []
    start_day = "2023-02-01"
    last_day = "2023-05-31"

    start_date = datetime.strptime(start_day, "%Y-%m-%d")
    last_date = datetime.strptime(last_day, "%Y-%m-%d")

    date_list = [datetime.strftime(start_date + timedelta(days=i), '%Y%m%d') for i in range((last_date - start_date).days + 1)]

    re_list.extend(parmap.map(ex_tag, date_list, pm_pbar=True, pm_processes=num_workers))

    tmp_list = [tag for tag_list in re_list for tag in tag_list]

    re_set = set(tmp_list)
    re_list = list(re_set)
    
    return re_list


def crawling(tag):
    article_dic = {}

    title_selector = "#title_area > span"
    content_selector = "#dic_area"

    try:
        html = requests.get(tag, headers = {"User-Agent": "Mozilla/5.0 "\
                "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"\
                "Chrome/110.0.0.0 Safari/537.36"})
    except:
        logging.error(traceback.format_exc())
    
    soup = BeautifulSoup(html.text, "lxml")

    title = soup.select(title_selector)
    title_list = [t.text for t in title]
    title_str = "".join(title_list)

    content = soup.select(content_selector)
    content_list = []

    for c in content:
        c_text = c.text
        c_text = c_text.strip()
        content_list.append(c_text)
    content_str = "".join(content_list)

    article_dic["TITLE"] = title_str
    article_dic["CONTENT"] = content_str

    return article_dic


def make_dic_list():
    article_dic_list = []

    re_list = re_tag()

    for tag in tqdm(re_list):
        try:
            article_dic_list.append(crawling(tag))
        except:
            logging.error(traceback.format_exc())
    
    return article_dic_list


def insertData(dic_list, coll):
    
    try:
        coll.insert_many(dic_list)
    except:
        logging.error(traceback.format_exc())

    return print("Insert Complete!")



def main():
    article_dic_list = make_dic_list()

    conn = pymongo.MongoClient(host='10.200.10.203', port=27017)
    db_name = "test_yw"
    coll_name = "news_ytn"
    db = conn.get_database(db_name)
    coll = db.get_collection(coll_name)

    insertData(article_dic_list, coll)

    conn.close()
    

if __name__=="__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()

    elapsed_time = end_time - start_time

    print("time : {}s".format(elapsed_time.total_seconds()))