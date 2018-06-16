import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol, PostStatus
from sqlalchemy import distinct
from multiprocessing import Pool

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).all()
    uids = [str(uid[0]) for uid in uids]
    #uids = [1036663592]

    for uid in uids:
        weibo.getUserTimelineOther(uid,start_day=-1)