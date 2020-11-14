import requests
from model import session, User, UserRelationship, \
    InfoQueue, FollowQueue, init_db, drop_db, restart_session, \
        FOLLOWER, FOLLOWEE
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
import re
import time
import datetime
import sys
import logging
from bs4 import BeautifulSoup

INFO_QUEUE_CAPACITY = 200000
FOLLOW_QUEUE_CAPACITY = 200000

info_url_pattern = "https://weibo.cn/{}/info"
profile_url_pattern = "https://weibo.cn/{}/profile"
follower_url_pattern = "https://weibo.cn/{}/fans?page={}"
followee_url_pattern = "https://weibo.cn/{}/follow?page={}"

chat_re_pattern = re.compile("https://weibo.cn/im/chat\?uid=(.*)&.*")
profile_re_pattern = re.compile("^(.*)\s./(.*)\s.*")
info_re_pattern = re.compile(".*昵称:(.*).*性别:(.).*")
uid_re_pattern = re.compile("https://weibo.cn/u/(.*)")
avatar_re_pattern = re.compile("/(.*)/avatar.*")

headers = {
  'Cookie': 'SUB=_2A25yj6m6DeRhGedJ7VYY9inFzDuIHXVuczfyrDV6PUJbkdAKLVGmkW1NUcH3X01qpMY-RGIAywustUDdgmABOOH0; SUHB=0IIUVQoBwlYNn6; _T_WM=63882731434',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'
}

def fetch_info(user_url):
    uid = None
    response = requests.request("GET", user_url, headers=headers)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        info_str = soup.select("div[class='u'] table td span[class='ctt']")[0].get_text()
        uid = avatar_re_pattern.findall(soup.select("div[class='u'] table td a")[0]['href'])[0]
        name, location = profile_re_pattern.findall(info_str)[0]
        logging.info("uid: {}, name: {}, location: {}".format(uid, name, location))
        try:
            user = User(uid, name, location)
            queue_follow(uid)
            # ele = FollowQueue(uid)
            session.add(user)
            # session.add(ele)
            session.commit()
        except IntegrityError:
            session.rollback()
            logging.info("repeat primary key")
    except IndexError:
        logging.error("Index out of range")
        t = 0
        with open('log/info_consumer/index_error_times.log', 'r') as f:
            t = int(f.read())+1
            if t>4:
                first_info_obj = session.query(InfoQueue).order_by(InfoQueue.create_time).first()
                session.delete(first_info_obj)
                session.commit()
                logging.info('remove first obj in info queue since there is some problems')
        with open('log/info_consumer/index_error_times.log', 'w') as f:
            f.write(str(t))
        exit(-1)

    return uid

def fetch_profile(user_url):
    response = requests.request("GET", user_url, headers=headers)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        info_str = soup.select("div[class='u'] table td span[class='ctt']")[0].get_text()
        name, location = profile_re_pattern.findall(info_str)[0]
        logging.info("name: {}, location: {}".format(name, location))
        try:
            user = User(uid, name, location)
            ele = FollowQueue(uid)
            session.add(user)
            session.add(ele)
            session.query(InfoQueue).filter(InfoQueue.uid==uid).delete()
            logging.info("info dequeued {}".format(uid))
            session.commit()
        except IntegrityError:
            session.rollback()
            logging.info("repeat primary key")
    except IndexError:
        logging.info("Index out of range")
    
    
def fetch_followers(uid):
    response = requests.request("GET", follower_url_pattern.format(uid, 1), headers=headers)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        total_page_num = int(soup.select("#pagelist input[name='mp']")[0]['value'])
        fan_uid_list = []
        for page in range(1, total_page_num+1):
            refresh_count = 0
            fan_list = []
            while len(fan_list)==0 and refresh_count < 4:
                time.sleep(1)
                logging.info("{}'s follower page {} begin. refresh count {}".format(uid, page, refresh_count))
                response = requests.request("GET", follower_url_pattern.format(uid, page), headers=headers)
                soup = BeautifulSoup(response.text, features="lxml")
                refresh_count+=1
                fans_list = soup.select("table tr td[style='width: 52px'] a")
            if len(fan_list)==0:
                logging.info("{}'s follower page {} is empty. break".format(uid, page))
                break
            for fan_info in fans_list:
                # uid_arr = chat_re_pattern.findall(fan_info['href'])
                user_url = fan_info['href']
                try:
                    queue_info(user_url, FOLLOWER, uid)
                    logging.info("user {}, follower of {}, has been fetched".format(user_url, uid))
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    logging.info("repeat primary key {}".format(user_url))
            logging.info("{}'s follower page {} finish.".format(uid, page))
    except IndexError:
        logging.info("Index out of range")

def fetch_followees(uid):
    response = requests.request("GET", followee_url_pattern.format(uid, 1), headers=headers)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        total_page_num = int(soup.select("#pagelist input[name='mp']")[0]['value'])
        followee_uid_list = []
        for page in range(1, total_page_num+1):
            response = requests.request("GET", followee_url_pattern.format(uid, page), headers=headers)
            soup = BeautifulSoup(response.text, features="lxml")
            followees_list = soup.select("table tr td[style='width: 52px'] a")
            for followee_info in followees_list:
                user_url = followee_info['href']
                try:
                    queue_info(user_url, FOLLOWEE, uid)
                    logging.info("user {}, followee of {}, has been fetched".format(user_url, uid))
                except IntegrityError:
                    session.rollback()
                    logging.info("repeat primary key {}".format(user_url))
            logging.info("{}'s followee page {} finish.".format(uid, page))
            time.sleep(1)
    except IndexError:
        logging.info("Index out of range")

def queue_info(user_url, follow_or_fan, uid):
    count_info_queue = session.query(func.count('*')).select_from(InfoQueue).scalar()
    logging.info(count_info_queue)
    while count_info_queue > INFO_QUEUE_CAPACITY:
        logging.warning('info queue is full. waiting for being consumed...')
        count_info_queue = session.query(func.count('*')).select_from(InfoQueue).scalar()
        session.commit()
        logging.info(count_info_queue)
        time.sleep(10)
    ele = InfoQueue(user_url, follow_or_fan, uid)
    session.add(ele)
    session.commit()

def dequeue_info():
    first_info_obj = session.query(InfoQueue).order_by(InfoQueue.create_time).first()
    if first_info_obj != None:
        # uid = first_info_obj.uid
        user_url = first_info_obj.url
        logging.info("got first url in info queue {}".format(user_url))
        uid = fetch_info(user_url)
        logging.info("got info of {}".format(uid))
        relations_in_buffer = session.query(InfoQueue).filter(InfoQueue.url==user_url)\
                                                      .all()
        for relation in relations_in_buffer:
            relation_obj = None
            if relation.follow_or_fan == FOLLOWEE:
                relation_obj = UserRelationship(uid, relation.source_uid)
                logging.info("build relationship between {} and {}".format(uid, relation.source_uid))
            elif relation.follow_or_fan == FOLLOWER:
                relation_obj = UserRelationship(relation.source_uid, uid)
                logging.info("build relationship between {} and {}".format(relation.source_uid, uid))
            session.add(relation_obj)
            session.delete(relation)
            logging.info("dequeue relationship between {} and {}".format(relation.source_uid, uid))
        session.commit()

        with open('log/info_consumer/index_error_times.log', 'w') as f:
            f.write('0')

def queue_follow(uid):
    count_follow_queue = session.query(func.count('*')).select_from(FollowQueue).scalar()
    logging.info(count_follow_queue)
    while count_follow_queue > FOLLOW_QUEUE_CAPACITY:
        logging.warning('follow queue is full. waiting for being consumed...')
        count_follow_queue = session.query(func.count('*')).select_from(FollowQueue).scalar()
        session.commit()
        logging.info(count_follow_queue)
        time.sleep(2)
    ele = FollowQueue(uid)
    session.add(ele)
    session.commit()

def dequeue_follow():
    first_follow_obj = session.query(FollowQueue).order_by(FollowQueue.create_time).first()
    if first_follow_obj != None:
        uid = first_follow_obj.uid
        logging.info("got first uid in follow queue {}".format(uid))
        fetch_followees(uid)
        logging.info("got followee of {}".format(uid))
        time.sleep(1)
        fetch_followers(uid)
        logging.info("got follower of {}".format(uid))
        session.query(FollowQueue).filter(FollowQueue.uid==uid).delete()
        logging.info("follow dequeued {}".format(uid))
        session.commit()


if __name__=="__main__":
    role = sys.argv[1]
    logging.basicConfig(level=logging.INFO,
                    filename='log/{0}/{0}.log'.format(role),
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
    if role=='info_consumer':
        logging.info("Info Consumer!")
        while(True):
            dequeue_info()
            time.sleep(1)
    elif role=='follow_consumer':
        logging.info("Follow Consumer!")
        while(True):
            dequeue_follow()
            time.sleep(1)
    elif role=='test':
        logging.info("Test!")
        fetch_info("https://weibo.cn/u/5121443008")




