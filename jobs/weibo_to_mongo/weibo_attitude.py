import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
from pymongo import MongoClient
import threading

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = MongoClient()
    db = client.weibo
    postTable = db.weibo_user_post
    attitudeTable = db.weibo_user_attitude

    startTime = weibo.getStrTime(-1)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]


    pidList = list(postTable.find({'uid':{'$in':uidList},'created_at_timestamp':{'$gte':startTimeStamp}},{'id':1}))
    pidList = [pid['id'] for pid in pidList]

    pipeline = [
        {'$match': {'status.id': {'$in': pidList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'}}}
    ]
    attitudeList = list(attitudeTable.aggregate(pipeline))

    client.close()
    session.close()

    for attitude in attitudeList:
        weibo.logger.info('{}/{} is in progress!'.format(attitudeList.index(attitude) + 1, len(attitudeList)))
        weibo.getAttitudesShow(str(attitude['_id']), since_id = str(attitude['since_id']),count=100)
    """
    threads = []
    weibo.logger.info('{} posts to be updated'.format(len(pidList)))
    for pid in pidList:
        t = threading.Thread(target=weibo.getAttitudesShow,args = (str(pid['id']),), kwargs={'count':100,'position':pidList.index(pid)+1,'all':len(pidList)})
        #weibo.logger.info('{}/{} is in progress!'.format(pidList.index(pid)+1,len(pidList)))
        #weibo.getAttitudesShow(str(pid['id']),count=100)
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 50:
                break

    """
