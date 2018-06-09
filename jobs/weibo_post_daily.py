import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).filter(Kol.status == 1).all()
    uidList = [str(uid[0]) for uid in uids]

    for uid in uidList:
        weibo.getUserTimelineOther(uid,start_day=-7)