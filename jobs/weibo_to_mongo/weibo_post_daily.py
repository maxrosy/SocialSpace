import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import threading
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
    
    for uid in uids:
        t = threading.Thread(target=weibo.get_user_timeline_other, args=(uid,), kwargs={'start_day':-7,'count': 50})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 50:
                break
