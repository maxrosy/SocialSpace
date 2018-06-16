import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import PostStatus, Kol, User
from pymongo import MongoClient

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = MongoClient()
    db = client.weibo
    postTable = db.weibo_user_post

    startTime = weibo.getStrTime(-2)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]


    pidList = list(postTable.find({'uid':{'$in':uidList},'created_at':{'$gte':startTimeStamp}},{'id':1}))


    client.close()
    session.close()

    weibo.logger.info('{} posts to be updated'.format(len(pidList)))
    for pid in pidList:
        weibo.logger.info('{}/{} is in progress!'.format(pidList.index(pid)+1,len(pidList)))
        weibo.getAttitudesShow(str(pid['id']),count=100)


