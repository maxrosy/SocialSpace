from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
from pymongo import MongoClient
import threading
from multiprocessing import Pool

def func(pid,position,total):
    weibo.logger.info('{}/{}'.format(position,total))
    weibo.getStatusRepostTimeline(pid,count=50)

if __name__ == '__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = MongoClient()
    db = client.weibo
    postTable = db.weibo_user_post
    repostTable = db.weibo_user_repost

    startTime = weibo.getStrTime(-1)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    pidList = list(postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1}))
    pidList = [pid['id'] for pid in pidList]
    pipeline = [
        {'$match' : {'retweeted_status.id':{'$in':pidList}}},
        {'$group' : {'_id': '$retweeted_status.id','since_id':{'$max':'$id'}}}
    ]
    repostList = list(repostTable.aggregate(pipeline))
    client.close()
    session.close()

    weibo.logger.info('{} posts to be updated'.format(len(pidList)))

    for repost in repostList:
        weibo.logger.info('{}/{} is in progress!'.format(repostList.index(repost)+1, len(repostList)))
        weibo.getStatusRepostTimeline(str(repost['_id']),since_id=str(repost['since_id']),count=50)

    """
    # Multiporcessing
    p = Pool(4)

    for pid in pidList:
        res = p.apply_async(func,(str(pid['id']),pidList.index(pid)+1,len(pidList),))

    p.close()
    p.join()
    """
    """
    #Threading
    threads = []
    for pid in pidList:
        t = threading.Thread(target=weibo.getStatusRepostTimeline,args=(str(pid['id']),),kwargs={'count':50})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 50:
                break
    """

