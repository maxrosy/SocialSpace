import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import threading
from sqlalchemy import and_
import time

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).all()
    uids = [str(uid[0]) for uid in uids]
    session.close()
    #uids = ['5339305113','6883966016','1594052081','1747506517','1793285524','1647720105']
    """
    for uid in uids:
        weibo.getUserTimelineOtherTest(uid,start_day=-2,count=100)
    """
    threads = []
    #start_day = -1638  #2019-06-25  1637 2015-01-01
    #end_day = start_day+14
    for uid in uids:
        t = threading.Thread(target=weibo.get_user_timeline_other, args=(uid,), kwargs={'start_day':-7,'count': 50})
        #t = threading.Thread(target=weibo.get_user_timeline_other, args=(uid,), kwargs={'start_day':start_day,'end_day':end_day,'count': 50})

        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 20:
                break
