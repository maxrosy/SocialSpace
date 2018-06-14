import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol, PostStatus
from sqlalchemy import distinct

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).all()
    uids = [str(uid[0]) for uid in uids]


    for uid in uids:
        weibo.getUserTimelineOther(uid,start_day=-2)