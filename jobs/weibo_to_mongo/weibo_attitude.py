from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
from pymongo import MongoClient
from multiprocessing import Pool


if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo._client
    db = client.weibo
    postTable = db.weibo_user_post
    attitudeTable = db.weibo_user_attitude

    startTime = weibo.getStrTime(-1)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    session.close()

    pidList = postTable.find({'uid':{'$in':uidList},'created_at_timestamp':{'$gte':startTimeStamp}},{'id':1})
    pidList = [pid['id'] for pid in pidList]


    pipeline = [
        {'$match': {'status.id': {'$in': pidList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'}}}
    ]
    attitudeList = list(attitudeTable.aggregate(pipeline))


    # Append posts with no attitude in DB into list
    attitudePostList = [attitudePost['_id'] for attitudePost in attitudeList]
    for pid in pidList:
        if pid not in attitudePostList:
            attitudeList.append(dict(_id=pid,since_id=0))

    """
    for i,attitude in enumerate(attitudeList):
        weibo.logger.info('{}/{} is in progress!'.format(i + 1, len(attitudeList)))
        if attitude.get('since_id'):
            weibo.getAttitudesShow(str(attitude['_id']), client, since_id = str(attitude['since_id']),count=100)
        else:
            weibo.getAttitudesShow(str(attitude['_id']), client, count=100)
    """
    weibo.doParallel('attitude',attitudeList)
    weibo._client.close()


