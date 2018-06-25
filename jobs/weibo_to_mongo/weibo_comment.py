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
    commentTable = db.weibo_user_comment

    startTime = weibo.getStrTime(-30)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    pidList = postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1})
    pidList = [pid['id'] for pid in pidList]

    pipeline = [
        {'$match': {'status.id': {'$in': pidList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'}}}
    ]
    commentList = commentTable.aggregate(pipeline)

    client.close()
    session.close()

    for comment in commentList:
        #comment = {'_id':'4251874968745231','since_id':'4251923319744970'}
        weibo.logger.info('{}/{} is in progress!'.format(commentList.index(comment)+1, len(commentList)))
        weibo.getCommentsShow(str(comment['_id']),since_id=str(comment['since_id']),count=200)

    """
    threads = []

    weibo.logger.info('{} posts to be updated'.format(len(pidList)))
    for pid in pidList:
        t = threading.Thread(target= weibo.getCommentsShow,args=(str(pid['id']),), kwargs={'count':200})
        threads.append(t)


    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) <50:
                break
    """
