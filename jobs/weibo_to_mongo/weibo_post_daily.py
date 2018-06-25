import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import threading

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).all()
    uids = [str(uid[0]) for uid in uids]
    #uids = [1743040097]
    """
    for uid in uids:
        weibo.getUserTimelineOther(uid,start_day=-2,count=100)
    """
    threads = []

    for uid in uids:
        t = threading.Thread(target=weibo.getUserTimelineOther, args=(uid,), kwargs={'start_day':-2,'count': 50})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 50:
                break