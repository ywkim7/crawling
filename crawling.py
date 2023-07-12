import requests
import pymongo
import logging
import traceback
import multiprocessing
import parmap
import configparser
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tqdm import tqdm

# ytn(oid=052) 일자별 기사 링크 추출 메소드
def extract_tag(date):
    tag_list = []

    for page in range(31):
        # f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&sid={sid}&date={date}&page={page}"
        url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=052&date={date}&page={page}"
        # 서버에 의해 접속이 차단되는 것을 막기 위해 User-Agent 헤더 설정
        try:
            html = requests.get(url, headers={"User-Agent": "Mozilla/5.0 "\
                "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"\
                "Chrome/110.0.0.0 Safari/537.36"})
        except:
            logging.error(traceback.format_exc())
        soup = BeautifulSoup(html.text, 'lxml')

        # 한 페이지 당 기사가 20개, 본문과 사진의 a 태그가 가지고 있는 링크가 같으므로 최대 40번의 반복문을 실행
        for i in range(40):
            try:
                tag = soup.find_all('a', {'class':'nclicks(cnt_flashart)'})[i].attrs['href']
                tag_list.append(tag)
            except IndexError:
                break

    # set로 중복 제거
    tag_set = set(tag_list)
    tag_list = list(tag_set)

    return tag_list

# extracting_tag 메소드를 실행하고 추출된 링크들을 리스트 형태로 반환
def save_tag():
    num_workers = multiprocessing.cpu_count()

    re_list = []

    # 날짜 지정
    start_day = "2023-07-12"
    last_day = "2023-07-12"

    start_date = datetime.strptime(start_day, "%Y-%m-%d")
    last_date = datetime.strptime(last_day, "%Y-%m-%d")

    # 시작 날짜부터 마지막 날짜를 네이버 링크 형식에 맞게 변형하여 리스트에 저장
    date_list = [datetime.strftime(start_date + timedelta(days=i), '%Y%m%d') for i in range((last_date - start_date).days + 1)]

    # 가용한 cpu 갯수 만큼 멀티프로세싱을 활용하여 링크 추출
    re_list.extend(parmap.map(extract_tag, date_list, pm_pbar=True, pm_processes=num_workers))

    # 멀티프로세싱에 의해 이중 리스트가 생성되므로 다시 단일 리스트로 저장
    tag_list = [tag for tag_list in re_list for tag in tag_list]
    
    return tag_list

# 제목 및 본문을 크롤링하여 딕셔너리 형태로 반환
def crawling(tag):
    article_dic = {}

    title_selector = "#title_area > span"
    content_selector = "#dic_area"

    # 서버에 의해 접속이 차단되는 것을 막기 위해 User-Agent 헤더 설정
    try:
        html = requests.get(tag, headers = {"User-Agent": "Mozilla/5.0 "\
                "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"\
                "Chrome/110.0.0.0 Safari/537.36"})
    except:
        logging.error(traceback.format_exc())
    
    soup = BeautifulSoup(html.text, "lxml")

    # 제목 추출
    # 제목 내의 html 태그를 제외한 문자들만 뽑아 하나의 문자열로 만들어 줌
    title_list = [t.text for t in soup.select(title_selector)]
    title = "".join(title_list)

    # 본문 추출
    content_list = []

    # 본문 내의 html 태그를 제외한 문자들만 뽑아 하나의 문자열로 만들어 줌
    for c in soup.select(content_selector):
        c_text = c.text
        c_text = c_text.strip()
        content_list.append(c_text)
    content = "".join(content_list)

    # {"TITLE":제목, "CONTENT":본문}의 형태로 만들어 줌
    article_dic["TITLE"] = title
    article_dic["CONTENT"] = content

    return article_dic

# re_tag 메소드를 실행하여 기사들의 링크를 리스트 형태로 저장
# crawling 메소드를 실행하여 크롤링된 {"TITLE":제목, "CONTENT":본문} 형태의 데이터를 리스트에 저장
# 크롤링의 경우 멀티프로세싱을 활용할 시 호스트에 의해 연결이 끊기는 문제 발생
def make_dic_list():
    article_dic_list = []

    re_list = save_tag()

    for tag in tqdm(re_list):
        try:
            article_dic_list.append(crawling(tag))
        except:
            logging.error(traceback.format_exc())
    
    return article_dic_list

# MongoDB에 데이터를 Insert하는 메소드
def insertData(dic_list, coll):
    
    try:
        coll.insert_many(dic_list)
    except:
        logging.error(traceback.format_exc())

    return print("Insert Complete!")



def main():
    try:
        # config.ini 파일에 저장된 설정값들을 불러와 저장
        properties = configparser.ConfigParser()
        properties.read('./config.ini', encoding='utf-8')

        # 저장된 설정값들을 인자로 전달하여 MongoDB에 연결
        conn = pymongo.MongoClient(host=properties['ETC']['host'], port=int(properties['ETC']['port']))
        db_name = properties['ETC']['db_name']
        coll_name = properties['ETC']['coll_name']
        db = conn.get_database(db_name)
        coll = db.get_collection(coll_name)
    except:
        logging.error(traceback.format_exc())

    # 기사들을 크롤링하여 결과값 저장
    article_dic_list = make_dic_list()

    # 크롤링된 기사들을 MongoDB에 Insert
    insertData(article_dic_list, coll)
    conn.close()
    

if __name__=="__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()

    elapsed_time = end_time - start_time

    print("time : {}s".format(elapsed_time.total_seconds()))