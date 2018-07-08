from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol



if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo._client
    db = client.weibo
    postTable = db.weibo_user_post
    commentTable = db.weibo_user_comment

    startTime = weibo.getStrTime(-1)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    pidList = postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1,'comments_count':1})
    pidList = [pid['id'] for pid in pidList]


    pipeline = [
        {'$match': {'status.id': {'$in': pidList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'},'count':{'$sum':1}}}
    ]
    commentList = list(commentTable.aggregate(pipeline))

    session.close()

    # Append posts with no attitude in DB into list
    commentPostList = [commentPost['_id'] for commentPost in commentList]
    for pid in pidList:
        if pid not in commentPostList:
            commentList.append(dict(_id=pid,since_id=0))

    weibo.doParallel('comment',commentList)
    weibo._client.close()
