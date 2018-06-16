from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
from pymongo import MongoClient

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

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

    client.close()
    session.close()

    weibo.logger.info('{} posts to be updated'.format(len(pidList)))
    for pid in pidList:
        weibo.logger.info('{}/{} is in progress!'.format(pidList.index(pid)+1, len(pidList)))
        weibo.getStatusRepostTimeline('4251518594532675',count=50)