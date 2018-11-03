from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import pandas as pd
import numpy as np


if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo.client
    db = client.weibo
    postTable = db.weibo_user_post
    commentTable = db.weibo_user_comment

    startTime = weibo.getStrTime(-7)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]
    session.close()

    pidList = list(postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1,'comments_count':1}))
    pList = [pid['id'] for pid in pidList]

    df_commentsInPost = pd.DataFrame(list(pidList))
    pipeline = [
        {'$match': {'status.id': {'$in': pList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'}, 'count': {'$sum': 1}}}
    ]
    commentList = list(commentTable.aggregate(pipeline))


    if commentList:
        df_commentsInComment = pd.DataFrame(commentList)
        df = df_commentsInPost.merge(df_commentsInComment, left_on='id', right_on='_id', how='left')
        df['since_id'] = df['since_id'].replace(np.nan, 0)
        df['count'] = df['count'].replace(np.nan, 0)
        df = df[df['comments_count'] > df['count']]
    else:
        df = df_commentsInPost
        df['since_id'] = 0



    commentPostList = df[['id', 'since_id']].to_dict('records')


    weibo.doParallel('comment',commentPostList)
    weibo._client.close()
