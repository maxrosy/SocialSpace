from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from sqlalchemy.sql import exists
from SocialAPI.Model import MasterUid,MasterUidInitial
import threading
import datetime

def get_weibo_post(uids,start_day=-7,count_num=50):
    weibo = SocialWeiboAPI()

    threads = []

    for user in uids:
        uid = user.get('uid')
        if user.get('start_day') and isinstance(user.get('start_day'), datetime.datetime):
            start_day_num = (user.get('start_day') - datetime.datetime.now()).days
        elif user.get('start_day') and isinstance(user.get('start_day'),int):
            start_day_num = user.get('start_day')
        else:
            start_day_num = start_day
        t = threading.Thread(target=weibo.get_user_timeline_other, args=(uid,),
                             kwargs={'start_day': start_day_num, 'count': count_num})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 20:
                break

def weibo_post_initial():
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()
        uids = session.query(MasterUidInitial.uid, MasterUidInitial.crawl_from) \
            .filter(~exists().where(MasterUid.uid == MasterUidInitial.uid)) \
            .all()
        uids = [{'uid': str(_[0]), 'start_day': _[1]} for _ in uids]
        get_weibo_post(uids)

    finally:
        session.close()

def weibo_post_daily(start_day=-7):
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()
        uids = session.query(MasterUid.uid)\
            .filter(MasterUid.crawl_master==1,MasterUid.crawl_post==1)\
            .all()
        uids = [{'uid':str(_[0]),'start_day':start_day} for _ in uids]
        get_weibo_post(uids)

    finally:
        session.close()

if __name__ == '__main__':
    #weibo_post_initial()
    weibo_post_daily()