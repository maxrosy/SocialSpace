from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol



if __name__ == '__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo._client
    db = client.weibo
    postTable = db.weibo_user_post
    repostTable = db.weibo_user_repost

    startTime = weibo.getStrTime(-7)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    pidList = postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1})
    pidList = [pid['id'] for pid in pidList]
    pipeline = [
        {'$match' : {'retweeted_status.id':{'$in':pidList}}},
        {'$group' : {'_id': '$retweeted_status.id','since_id':{'$max':'$id'}}}
    ]
    repostList = list(repostTable.aggregate(pipeline))
    client.close()
    session.close()

    # Append posts with no attitude in DB into list
    repostPostList = [repostPost['_id'] for repostPost in repostList]
    for pid in pidList:
        if pid not in repostPostList:
            repostList.append(dict(_id=pid,since_id=0))

    """
    for i,repost in enumerate(repostList):
        weibo.logger.info('{}/{} is in progress!'.format(i+1, len(repostList)))
        if repost.get('since_id'):
            weibo.getStatusRepostTimeline(str(repost['_id']),client,since_id=str(repost['since_id']),count=50)
        else:
            weibo.getStatusRepostTimeline(str(repost['_id']),client, count=50)
    """
    weibo.doParallel('repost',repostList)
    weibo._client.close()

